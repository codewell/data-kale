def test_empty(
    repository,
):
    assert len(list(repository.objects.all())) == 0

def test_correct_key(
    repository,
    repository_name,
):
    assert repository.name == repository_name