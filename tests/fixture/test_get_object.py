def test_file(
    get_object,
):
    get_object('a.txt').write(b'a')
    assert get_object('a.txt').read() == b'a'

def test_directory(
    get_object,
):
    get_object('dir/').mkdir()

    assert get_object('dir/').isdir()