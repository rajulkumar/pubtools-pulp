from pushsource import Source, RpmPushItem


class FakeSource(Source):
    def __init__(self):
        self.pushitems = [
            "RHSA-1111:22: new module: rhel8",
            RpmPushItem(
                name="bash-1.23-1.test8_x86_64.rpm",
                signing_key="aabbcc",
                dest=["some-yumrepo"],
            ),
            RpmPushItem(
                name="dash-1.23-1.test8_x86_64.rpm",
                signing_key="aabbcc",
                dest=["some-yumrepo"],
            ),
        ]

    def __iter__(self):
        for item in self.pushitems:
            yield item
