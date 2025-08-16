# Channel Parameters

Channel parameters can be defined using the `Channel` annotation. A channel may contain expressions that can be used to
define dynamic values. Expressions must be made of a name enclosed in curly brackets. E.g. `{user_id}`.

```{eval-rst}
.. async-fast-example:: examples/channel_parameter.py
```

The value of the channel parameter user_id will be passed to your function as the argument user_id.
