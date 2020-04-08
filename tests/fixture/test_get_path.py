from pytest import raises


def test_file(
    get_path,
):
    get_path('a.txt').write(b'a')
    assert get_path('a.txt').read() == b'a'

def test_directory(
    get_path,
):
    get_path('dir/').mkdir()
    assert get_path('dir/').isdir()

def test_not_exists(
    get_path,
):
    assert not get_path('a.txt').exists()
    with raises(FileNotFoundError):
        get_path('b.txt').read()

def test_dir_not_exists(
    get_path,
):
    assert not get_path('dir/').exists()
    assert not get_path('dir/').isdir()
   
def test_read_directory(
    get_path,
):
    get_path('dir/').mkdir()
    with raises(IsADirectoryError):
        get_path('dir/').read()