from __future__ import annotations

import unittest

from app.services.forge_hosts import ForgeHostConfigError, parse_forge_hosts, resolve_forge_host


class ForgeHostRegistryTests(unittest.TestCase):
    def test_built_in_hosts_are_exact(self) -> None:
        hosts = parse_forge_hosts("")
        self.assertEqual(hosts["github.com"].kind, "github")
        self.assertEqual(hosts["gitlab.com"].kind, "gitlab")
        self.assertIsNone(resolve_forge_host("gitlab.example.com", ""))

    def test_extra_gitlab_host_is_registered(self) -> None:
        host = resolve_forge_host("git.acme.test", "git.acme.test=gitlab")
        self.assertIsNotNone(host)
        self.assertEqual(host.kind, "gitlab")
        self.assertEqual(host.api_root, "https://git.acme.test/api/v4")
        self.assertEqual(host.token_name, "GITLAB_TOKEN")

    def test_substring_gitlab_is_not_enough(self) -> None:
        self.assertIsNone(resolve_forge_host("notgitlab.example.com", ""))

    def test_invalid_entries_fail_closed(self) -> None:
        with self.assertRaisesRegex(ForgeHostConfigError, "host=gitlab"):
            parse_forge_hosts("git.acme.test")
        with self.assertRaisesRegex(ForgeHostConfigError, "kinds must be gitlab"):
            parse_forge_hosts("git.acme.test=github")
        with self.assertRaisesRegex(ForgeHostConfigError, "cannot redefine"):
            parse_forge_hosts("gitlab.com=gitlab")
        with self.assertRaisesRegex(ForgeHostConfigError, "bare hostnames"):
            parse_forge_hosts("https://git.acme.test=gitlab")


if __name__ == "__main__":
    unittest.main()
