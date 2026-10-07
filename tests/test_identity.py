from remoteslurm import identity


def test_source_identity_includes_subpackages(tmp_path, monkeypatch):
    root = tmp_path / "package"
    (root / "workspace").mkdir(parents=True)
    (root / "identity.py").write_text("# fixture")
    nested = root / "workspace/service.py"
    nested.write_text("version = 1")
    monkeypatch.setattr(identity, "__file__", str(root / "identity.py"))
    identity.source_sha.cache_clear()
    try:
        before = identity.source_sha()
        nested.write_text("version = 2")
        identity.source_sha.cache_clear()
        assert identity.source_sha() != before
    finally:
        identity.source_sha.cache_clear()
