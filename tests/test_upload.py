from data_kale.upload import upload


def test_upload(
    root,
    repository_name,
    get_path,
    get_object,
):
    get_path('a.txt').write(b'a')
    get_path('b.txt').write(b'b')
    get_path('dir/').mkdir()
    get_path('dir/c.txt').write(b'c')

    upload(repository_name, root=root)

    assert get_object('a.txt').read() == b'a'
    assert get_object('b.txt').read() == b'b'
    assert get_object('dir/c.txt').read() == b'c'