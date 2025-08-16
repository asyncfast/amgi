# Message Payload

In a decorated channel any argument that is not a channel parameter, or header is treated as the payload.

```{important}
Only one payload argument is allowed, there is no attempt to merge multiple, istead an error will be raised
```

## BaseModel

You can declare a data model as a class that inherits from `BaseModel`. This can then be used as the payload argument:

```{eval-rst}
.. async-fast-example:: examples/payload_basemodel.py
```

## List

```{eval-rst}
.. async-fast-example:: examples/payload_list.py
```

## Dataclass

```{eval-rst}
.. async-fast-example:: examples/payload_dataclass.py
```

## Built in

```{eval-rst}
.. async-fast-example:: examples/payload_builtin.py
```
