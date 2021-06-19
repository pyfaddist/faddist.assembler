# faddist.assembler

###
```python
import json
from faddist.assembler import Assembler

assembler = Assembler(working_dir='/opt/project')
with open('/opt/project/pipeline.json', 'r') as fd:
    pipeline_configuration = json.load(fd)

pipeline = assembler.build_pipeline(pipeline_configuration)
pipeline.operate()

data = assembler.get_variable('bucket')
print(data)
```

### Pipeline configuration

```json
{
  "include": [
    "/etc/faddist-assembler/definitions/pydash-callables.json",
    "./definitions.json"
  ],
  "variables": [
    {
      "name": "bucket",
      "__type__": "list"
    },
    {
      "name": "engine",
      "__type__": "sqlalchemy.create_engine",
      "arguments": "sqlite:///test.db"
    }
  ],
  "iterator": {
    "__alias__": "db_reader",
    "arguments": [
      "$var:engine",
      "SELECT * FROM test"
    ]
  },
  "pipe": [
    {
      "__type__": "rx.operators.map",
      "arguments": [
        "$lambda x: {'a': '2021-05-' + str(x), 'x': x}"
      ]
    },
    {
      "__alias__": "transform",
      "arguments": [
        {
          "origin": "x",
          "date": "$lambda x: DateTime.strptime(x['a'], '%Y-%m-%d').date()",
          "other": {
            "__alias__": "parse_date",
            "arguments": [ "a", ["%m/%d%/%Y", "%Y-%m-%d"] ]
          }
        }
      ]
    }
  ],
  "observer": "$lambda x: bucket.append(x)"
}
```

### Definitions

```json
{
  "alias": [
    {
      "__type__": "datetime.date",
      "name": "Date"
    },
    {
      "__type__": "datetime.datetime",
      "name": "DateTime"
    },
    {
      "__type__": "faddist.rx.operators.TransformBuilder",
      "name": "transform"
    },
    {
      "__type__": "test_rx_operators.sample_transform_callable",
      "name": "parse_date"
    }
  ]
}
```