from data_kale.list_remote import list_remote


def test_list_remote(
   repository_name 
):
    assert repository_name in list_remote()