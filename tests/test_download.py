from data_kale.download import download


def test_download(
    root,
    repository_name,
    get_object,
    get_path,
):
    get_object('a.txt').write(b'a')
    get_object('b.txt').write(b'b')
    get_object('dir/').mkdir()

    download(repository_name, root=root)

    assert get_path('a.txt').read() == b'a'
    assert get_path('b.txt').read() ==  b'b'
    assert get_path('dir/').isdir()