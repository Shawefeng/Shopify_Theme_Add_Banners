"""Offline regression checks for Shopify searches and request retries."""
import ast
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import requests

SOURCE = Path(__file__).resolve().parents[1] / "retail_promotions_to_shopify_metafields.py"
tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
names = {"normalize", "escape_search_value", "ShopifyClient"}
nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
namespace = dict(Dict=Dict, List=List, Optional=Optional, Tuple=Tuple)
exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), namespace)
Client = namespace["ShopifyClient"]


def client(*responses):
    instance = Client.__new__(Client)
    instance.graphql = Mock(side_effect=responses)
    return instance


def collections(nodes, more=False, cursor=None):
    return {"data": {"collections": {"nodes": nodes, "pageInfo": {"hasNextPage": more, "endCursor": cursor}}}}


def products(nodes, more=False, cursor=None):
    return {"data": {"products": {"nodes": nodes, "pageInfo": {"hasNextPage": more, "endCursor": cursor}}}}


class LiteralSearchTests(unittest.TestCase):
    def test_literal_escaping(self):
        escape = namespace.get("escape_search_value")
        self.assertIsNotNone(escape)
        for value, expected in [
            ("Aster Lighting", "Aster Lighting"),
            ('ACME "Studio"', r'ACME \"Studio\"'),
            (r"ACME: (West)\North", r"ACME\: \(West\)\\North"),
            ("Dolan's", r"Dolan\'s"),
            ("Éclairage", "Éclairage"),
        ]:
            with self.subTest(value=value):
                self.assertEqual(escape(value), expected)

    def test_quoted_collection_title_and_exact_filter(self):
        title = 'ACME "Studio"'
        shop = client(collections([{"id": "other", "title": "Other"}, {"id": "wanted", "title": title}]))
        self.assertEqual(shop.find_collection_by_title_exact(title), ("wanted", title))
        self.assertEqual(shop.graphql.call_args.args[1]["q"], r'title:"ACME \"Studio\""')
        self.assertEqual(shop.graphql.call_count, 1)

    def test_collection_fallback_keeps_literal_characters(self):
        title = r"ACME: (West)\North"
        shop = client(collections([]), collections([{"id": "wanted", "title": title}]))
        self.assertEqual(shop.find_collection_by_title_exact(title), ("wanted", title))
        self.assertEqual(shop.graphql.call_args_list[0].args[1]["q"], r'title:"ACME\: \(West\)\\North"')
        self.assertEqual(shop.graphql.call_args_list[1].args[1]["q"], r"title:ACME\: \(West\)\\North")

    def test_unmatched_collection_still_returns_none(self):
        shop = client(collections([]), collections([{"id": "other", "title": "Different"}]))
        self.assertIsNone(shop.find_collection_by_title_exact("Aster Lighting"))

    def test_vendor_query_survives_pagination_and_retains_exact_filter(self):
        vendor = 'ACME "Studio": (West)\\North'
        shop = client(
            products([{"id": "p1", "vendor": vendor}, {"id": "wrong", "vendor": "Other"}], True, "next"),
            products([{"id": "p2", "vendor": vendor.lower()}]),
        )
        self.assertEqual(shop.list_product_ids_by_vendor(vendor), ["p1", "p2"])
        for call in shop.graphql.call_args_list:
            self.assertEqual(call.args[1]["q"], r'vendor:"ACME \"Studio\"\: \(West\)\\North"')
        self.assertIsNone(shop.graphql.call_args_list[0].args[1]["cursor"])
        self.assertEqual(shop.graphql.call_args_list[1].args[1]["cursor"], "next")

    def test_collection_phrase_search_continues_before_fallback(self):
        shop = client(
            collections([{"id": f"other-{i}", "title": f"Aster Lighting Sale {i}"} for i in range(20)], True, "phrase-next"),
            collections([{"id": "wanted", "title": "Aster Lighting"}]),
        )
        self.assertEqual(shop.find_collection_by_title_exact("Aster Lighting"), ("wanted", "Aster Lighting"))
        calls = shop.graphql.call_args_list
        self.assertEqual(calls[0].args[1]["q"], calls[1].args[1]["q"])
        self.assertEqual(calls[1].args[1]["cursor"], "phrase-next")

    def test_collection_fallback_also_checks_later_pages(self):
        shop = client(
            collections([]),
            collections([{"id": "other", "title": "Other"}], True, "fallback-next"),
            collections([{"id": "wanted", "title": "Aster Lighting"}]),
        )
        self.assertEqual(shop.find_collection_by_title_exact("Aster Lighting"), ("wanted", "Aster Lighting"))
        calls = shop.graphql.call_args_list
        self.assertIsNone(calls[1].args[1]["cursor"])
        self.assertEqual(calls[2].args[1]["cursor"], "fallback-next")
        self.assertEqual(calls[1].args[1]["q"], calls[2].args[1]["q"])

    def test_plain_vendor_query_is_unchanged(self):
        shop = client(products([]))
        self.assertEqual(shop.list_product_ids_by_vendor("Aster Lighting"), [])
        self.assertEqual(shop.graphql.call_args.args[1]["q"], 'vendor:"Aster Lighting"')


class GraphQLRetryTests(unittest.TestCase):
    def setUp(self):
        self.shop = Client.__new__(Client)
        self.shop.endpoint = "https://example.invalid/graphql.json"
        self.post = Mock()
        self.sleep = Mock()
        globals_patch = patch.dict(namespace, {
            "Config": SimpleNamespace(SHOPIFY_TOKEN="test-token", REQUEST_TIMEOUT=1),
            "requests": SimpleNamespace(post=self.post, HTTPError=requests.HTTPError),
            "time": SimpleNamespace(sleep=self.sleep),
        })
        globals_patch.start()
        self.addCleanup(globals_patch.stop)

    @staticmethod
    def response(status):
        response = requests.Response()
        response.status_code = status
        response.url = "https://example.invalid/graphql.json"
        response._content = b'{"data": {"shop": {"name": "Example"}}}'
        return response

    def test_authentication_and_forbidden_errors_fail_without_retry(self):
        for status in (401, 403):
            with self.subTest(status=status):
                response = self.response(status)
                self.post.reset_mock(side_effect=True)
                self.sleep.reset_mock()
                self.post.return_value = response
                with self.assertRaises(requests.HTTPError) as error:
                    self.shop.graphql("query { shop { name } }")
                self.assertIs(error.exception.response, response)
                self.post.assert_called_once()
                self.sleep.assert_not_called()

    def test_temporary_http_errors_and_network_timeouts_still_recover(self):
        for failure in (self.response(429), self.response(503), requests.Timeout("Timed out")):
            with self.subTest(failure=str(failure)):
                self.post.reset_mock()
                self.sleep.reset_mock()
                self.post.side_effect = [failure, self.response(200)]
                result = self.shop.graphql("query { shop { name } }")
                self.assertEqual(result["data"]["shop"]["name"], "Example")
                self.assertEqual(self.post.call_count, 2)
                self.sleep.assert_called_once()

    def test_persistent_temporary_error_respects_retry_limit(self):
        self.post.return_value = self.response(503)
        with self.assertRaisesRegex(RuntimeError, "Temporary Shopify error 503"):
            self.shop.graphql("query { shop { name } }", retries=2)
        self.assertEqual(self.post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
