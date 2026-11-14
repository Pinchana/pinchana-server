import unittest

from pinchana_server import main


class TikTokRoutingTests(unittest.TestCase):
    def test_bundled_module_routes_new_and_legacy_share_urls(self):
        for url in (
            "https://www.tt.site/t/ZGdC5nG4S/",
            "https://tt.site/t/ZGdC5nG4S/",
            "https://vm.tiktok.com/ZGdC5nG4S/",
            "https://vt.tiktok.com/ZGdC5nG4S/",
        ):
            with self.subTest(url=url):
                self.assertIs(main._resolve_module(url), main.modules["tiktok"])
