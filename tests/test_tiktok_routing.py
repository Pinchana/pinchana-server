import unittest
from pathlib import Path
from unittest.mock import patch

from pinchana_core.docker_manager import ContainerRegistry
from pinchana_core.plugins import PluginRegistry, ScraperPlugin
from pinchana_server import main
from fastapi import APIRouter


MODULES_CONFIG = Path(__file__).resolve().parents[2] / "config" / "modules.yaml"


class TikTokRoutingTests(unittest.TestCase):
    @unittest.skipUnless(MODULES_CONFIG.exists(), "parent API module config is not available")
    def test_configured_container_routes_new_and_legacy_share_urls(self):
        containers = ContainerRegistry(str(MODULES_CONFIG))
        with (
            patch.object(main, "registry", PluginRegistry()),
            patch.object(main, "container_registry", containers),
        ):
            for url in (
                "https://www.tt.site/t/ZGdC5nG4S/",
                "https://tt.site/t/ZGdC5nG4S/",
                "https://vm.tiktok.com/ZGdC5nG4S/",
                "https://vt.tiktok.com/ZGdC5nG4S/",
            ):
                with self.subTest(url=url):
                    mode, name, module = main._resolve_module(url)
                    self.assertEqual((mode, name), ("container", "tiktok"))
                    self.assertIs(module, containers.modules["tiktok"])

    def test_in_process_route_supports_new_domain(self):
        registry = PluginRegistry()
        plugin = ScraperPlugin(
            name="tiktok", router=APIRouter(), route_patterns=["tiktok.com", "tt.site"],
        )
        registry.register(plugin)
        with patch.object(main, "registry", registry):
            mode, name, target = main._resolve_module("https://www.tt.site/t/ZGdC5nG4S/")
        self.assertEqual((mode, name), ("in_process", "tiktok"))
        self.assertIs(target, plugin)
