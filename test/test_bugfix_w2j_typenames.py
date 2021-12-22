import pytest
from sqlalchemy import func, select


@pytest.mark.parametrize(
    "input_,output_",
    [
        (
            """[{"name":"id","type":"bigint","value":1},{"name":"mood","type":""MOOD"","value":"happy"}, {"name":"mood","type":""aB3c D"","value":"happy"}""",
            """[{"name":"id","type":"bigint","value":1},{"name":"mood","type":"MOOD","value":"happy"}, {"name":"mood","type":"aB3c D","value":"happy"}""",
        )
    ],
)
def test_bugfix_w2j_typeames(input_: str, output_: str, sess):
    x = sess.execute(select([func.realtime.bugfix_w2j_typenames(input_)])).scalar_one()
    assert x == output_
