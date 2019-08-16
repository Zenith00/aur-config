import aursync.flattener as flattener

from abc import ABC, abstractmethod


class _ConfigProxy(ABC):
    @abstractmethod
    def ready(self) -> bool: pass

    @abstractmethod
    def __setitem__(self, k: flattener.FlatKey, v: str) -> None: pass

    @abstractmethod
    def compose_key(self, k) -> None: pass

    @abstractmethod
    def __delitem__(self, k: flattener.FlatKey) -> None: pass

    def __getitem__(self, k: flattener.FlatKey) -> "_ConfigProxy":
        self.ready()
        self.compose_key(k)
        return self

    @abstractmethod
    def __str__(self) -> str: pass


class ConfigObjProxy(_ConfigProxy):
    from configobj import ConfigObj
    import pathlib

    def __init__(self, filename: pathlib.Path):
        self.conf = self.ConfigObj(filename)
        self.curr = self.conf

    def __setitem__(self, k: flattener.FlatKey, v: str) -> None:
        self.curr[k] = v
        self.conf.write()

    def compose_key(self, k) -> None:
        self.curr = self.curr[k]

    def __delitem__(self, k: flattener.FlatKey) -> None:
        del self.curr[k]

    def __getitem__(self, k: flattener.FlatKey) -> "_ConfigProxy":
        self.compose_key(k)
        return self

    def __str__(self) -> str:
        return str(self.curr)

    def ready(self) -> bool:
        return True


class AurSyncProxy(_ConfigProxy):
    import aursync
    import aursync.flattener as flattener

    def __init__(self, sync: aursync.Sync, key_root):
        self.key_root: str = key_root
        self.hm_key = ""
        self.sync = sync

    def ready(self) -> bool:
        self._verify_redis()
        return True

    def _verify_redis(self) -> None:
        if not self.sync.ready:
            raise RuntimeError("Sync redis not ready!")
        if not self.sync.redis:
            raise RuntimeError("<???>: Sync is ready but redis is None")
        if self.sync.redis.closed:
            raise RuntimeError("Sync redis is closed!")

    def __setitem__(self, k: flattener.FlatKey, v: str) -> None:
        self._verify_redis()
        self.compose_key(k)

        assert self.sync.redis is not None  # dummy for Mypy
        self.sync.redis.hmset(self.key_root, self.hm_key, v)

    def compose_key(self, k) -> None:
        sep: flattener.FlatContainerType
        sep = list if isinstance(k, int) else dict
        self.hm_key = flattener.compose_keys(self.hm_key, k, sep)

    def __delitem__(self, k: flattener.FlatKey) -> None:
        self._verify_redis()
        self.compose_key(k)

        assert self.sync.redis is not None  # dummy for Mypy
        self.sync.redis.hdel(self.key_root, self.hm_key)

    def __str__(self) -> str:
        assert self.sync.redis is not None  # dummy for  Mypy
        return self.sync.redis.hmget(self.key_root, self.hm_key)
