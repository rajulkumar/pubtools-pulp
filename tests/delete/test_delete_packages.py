from more_executors.futures import f_return
from fastpurge import FastPurgeClient

from pubtools.pulplib import (
    FakeController,
    Client,
    YumRepository,
    RpmUnit,
    ModulemdUnit,
    FileRepository,
    FileUnit,
)

from pubtools._pulp.ud import UdCacheClient

from pubtools._pulp.tasks.delete import DeletePackages


class FakeUdCache(object):
    def __init__(self):
        self.flushed_repos = []
        self.flushed_products = []

    def flush_repo(self, repo_id):
        self.flushed_repos.append(repo_id)
        return f_return()

    def flush_product(self, product_id):
        self.flushed_products.append(product_id)
        return f_return()


class FakeFastPurge(object):
    def __init__(self):
        self.purged_urls = []

    def purge_by_url(self, urls):
        self.purged_urls.extend(urls)
        return f_return()


class FakeDeletePackages(DeletePackages):
    """clear-repo with services overridden for test"""

    def __init__(self, *args, **kwargs):
        super(FakeDeletePackages, self).__init__(*args, **kwargs)
        self.pulp_client_controller = FakeController()
        self._udcache_client = FakeUdCache()
        self._fastpurge_client = FakeFastPurge()

    @property
    def pulp_client(self):
        # Super should give a Pulp client
        assert isinstance(super(FakeDeletePackages, self).pulp_client, Client)
        # But we'll substitute our own
        return self.pulp_client_controller.client

    @property
    def udcache_client(self):
        # Super may or may not give a UD client, depends on arguments
        from_super = super(FakeDeletePackages, self).udcache_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, UdCacheClient)

        # We'll substitute our own, only if UD client is being used
        return self._udcache_client if from_super else None

    @property
    def fastpurge_client(self):
        # Super may or may not give a fastpurge client, depends on arguments
        from_super = super(FakeDeletePackages, self).fastpurge_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, FastPurgeClient)

        # We'll substitute our own, only if fastpurge client is being used
        return self._fastpurge_client if from_super else None


def test_delete_rpms(command_tester, fake_collector, monkeypatch):
    """Clearing a repo with yum content succeeds."""

    repo = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )

    files = [
        RpmUnit(
            name="bash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="bash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
        ),
        RpmUnit(
            name="dash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="dash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
        ),
        ModulemdUnit(
            name="mymod", stream="s1", version=123, context="a1c2", arch="s390x"
        ),
    ]

    with FakeDeletePackages() as task_instance:

        task_instance.pulp_client_controller.insert_repository(repo)
        task_instance.pulp_client_controller.insert_units(repo, files)

        # Let's try setting the cache flush root via env.
        monkeypatch.setenv("FASTPURGE_ROOT_URL", "https://cdn.example2.com/")

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--fastpurge-host",
                "fakehost-xxx.example.net",
                "--fastpurge-client-secret",
                "abcdef",
                "--fastpurge-client-token",
                "efg",
                "--fastpurge-access-token",
                "tok",
                "--repo",
                "some-yumrepo",
                "--repo",
                "some-other-repo",
                "--file",
                "bash-1.23-1.test8_x86_64.rpm",
                "--signing-key",
                "aabbcc",
            ],
        )

    # It should record that it removed these push items:
    assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
        {
            "origin": "pulp",
            "src": None,
            "state": "DELETED",
            "build": None,
            "dest": None,
            "checksums": {"sha256": "a" * 64},
            "signing_key": None,
            "filename": "bash-1.23-1.test8.x86_64.rpm",
        }
    ]


def test_delete_modules(command_tester, fake_collector, monkeypatch):
    """Clearing a repo with yum content succeeds."""

    repo = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )

    files = [
        RpmUnit(
            name="bash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="bash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
        ),
        RpmUnit(
            name="dash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="dash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
        ),
        ModulemdUnit(
            name="mymod", stream="s1", version=123, context="a1c2", arch="s390x"
        ),
    ]

    with FakeDeletePackages() as task_instance:

        task_instance.pulp_client_controller.insert_repository(repo)
        task_instance.pulp_client_controller.insert_units(repo, files)

        # Let's try setting the cache flush root via env.
        monkeypatch.setenv("FASTPURGE_ROOT_URL", "https://cdn.example2.com/")

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--fastpurge-host",
                "fakehost-xxx.example.net",
                "--fastpurge-client-secret",
                "abcdef",
                "--fastpurge-client-token",
                "efg",
                "--fastpurge-access-token",
                "tok",
                "--repo",
                "some-yumrepo",
                "--file",
                "mymod:s1:123:a1c2:s390x",
            ],
        )

        assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
            {
                "origin": "pulp",
                "src": None,
                "state": "DELETED",
                "build": None,
                "dest": None,
                "checksums": None,
                "signing_key": None,
                "filename": "mymod:s1:123:a1c2:s390x",
            }
        ]


def test_delete_files(command_tester, fake_collector, monkeypatch):

    repo = FileRepository(
        id="some-filerepo",
        eng_product_id=123,
        relative_url="some/publish/url",
        mutable_urls=["mutable1", "mutable2"],
    )

    files = [
        FileUnit(path="hello.iso", size=123, sha256sum="a" * 64),
        FileUnit(path="some.iso", size=454435, sha256sum="b" * 64),
    ]

    with FakeDeletePackages() as task_instance:

        task_instance.pulp_client_controller.insert_repository(repo)
        task_instance.pulp_client_controller.insert_units(repo, files)

        # Let's try setting the cache flush root via env.
        monkeypatch.setenv("FASTPURGE_ROOT_URL", "https://cdn.example2.com/")

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--fastpurge-host",
                "fakehost-xxx.example.net",
                "--fastpurge-client-secret",
                "abcdef",
                "--fastpurge-client-token",
                "efg",
                "--fastpurge-access-token",
                "tok",
                "--repo",
                "some-filerepo",
                "--file",
                "some.iso,hello.iso",
            ],
        )

        assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
            {
                "origin": "pulp",
                "src": None,
                "state": "DELETED",
                "build": None,
                "dest": None,
                "checksums": {
                    "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                },
                "signing_key": None,
                "filename": "hello.iso",
            },
            {
                "origin": "pulp",
                "src": None,
                "state": "DELETED",
                "build": None,
                "dest": None,
                "checksums": {
                    "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                },
                "signing_key": None,
                "filename": "some.iso",
            },
        ]
