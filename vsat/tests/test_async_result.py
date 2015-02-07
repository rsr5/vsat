
import pytest

from os.path import exists, join

from vsat.task import AsyncResult, RESULT_LOCATION, NotReady


def test_result_file_locally():
    """
    Tests that not passing a uuid creates a new result file.
    """
    result = AsyncResult.create("atask", [], {})

    assert result.task_name == "atask"

    assert join(RESULT_LOCATION, result.task_uuid) == result.path

    assert exists(result.path)

    assert result.get_state() == "CREATED"

    with pytest.raises(NotReady):
        result.get_result()

    result.set_state("PROGRESS")

    assert result.get_state() == "PROGRESS"

    result.set_result(55555)

    assert result.get_result() == 55555

    assert not exists(result.path)
