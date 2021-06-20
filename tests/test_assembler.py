from sqlalchemy.engine import Engine

from faddist.assembler import Assembler
from faddist.sqlalchemy.rx.operators import get_current_session
from utils import absolute_path_to


def test_assembler_bootstrap():
    assembler = Assembler()
    assembler.bootstrap({'alias': [{'__type__': 'string.Template', 'name': 'Template'},
                                   {'__type__': 'datetime.datetime', 'name': 'Timestamp'}],
                         'variables': [{'__type__': 'string.Template', 'name': 'temp', 'arguments': ['sample $var']},
                                       {'__alias__': 'Timestamp', 'name': 'ts',
                                        'arguments': [2021, 6, 13, 15, 12, 9]}]})

    assert assembler.has_class('Template')
    assert assembler.has_class('Timestamp')

    from string import Template
    assert assembler.get_class('Template') == Template
    from datetime import datetime
    assert assembler.get_class('Timestamp') == datetime
    assert assembler.new_instance('Timestamp', [2021, 6, 13, 14, 45, 11]) == datetime(2021, 6, 13, 14, 45, 11)

    assert assembler.has_variable('temp')
    assert assembler.get_variable('temp').substitute(var='variable') == 'sample variable'
    assert assembler.has_variable('ts')
    assert assembler.get_variable('ts') == datetime(2021, 6, 13, 15, 12, 9)


def test_assembler_create_pipeline():
    assembler = Assembler()

    pipeline = assembler.build_pipeline({'variables': [{'__type__': 'list', 'name': 'test'},
                                                       {'__type__': 'int', 'name': 'start', 'arguments': '10'},
                                                       {'__type__': 'int', 'name': 'stop', 'arguments': '20'}],
                                         'iterator': {'__type__': 'range', 'arguments': ['$var:start', '$var:stop']},
                                         'pipe': [
                                             {'__type__': 'rx.operators.map', 'arguments': ['$lambda x: x*2']}
                                         ],
                                         'observer': '$lambda x: test.append(x)'})
    assert pipeline is not None
    assert assembler.has_variable('test')

    data = []
    pipeline.operate(lambda x: data.append(x))
    assert len(data) == 10
    assert data[0] == 20
    assert data[9] == 38

    data = assembler.get_variable('test')
    assert len(data) == 10
    assert data[0] == 20
    assert data[9] == 38


def test_read_file_like_object(northwind: Engine):
    assembler = Assembler(working_dir=absolute_path_to('samples'))
    assembler.set_variable('engine', northwind)

    pipeline = assembler.load_json_file('pipe.complex.json')
    pipeline.operate()

    bucket = assembler.get_variable('bucket')
    assert len(bucket) == 830
