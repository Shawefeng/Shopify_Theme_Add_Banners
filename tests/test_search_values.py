"""Offline regression checks for literal Shopify search values."""
import ast
import unittest
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import Mock

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


def collections(nodes):
    return {"data": {"collections": {"nodes": nodes}}}


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

    def test_plain_vendor_query_is_unchanged(self):
        shop = client(products([]))
        self.assertEqual(shop.list_product_ids_by_vendor("Aster Lighting"), [])
        self.assertEqual(shop.graphql.call_args.args[1]["q"], 'vendor:"Aster Lighting"')


if __name__ == "__main__":
    unittest.main()
