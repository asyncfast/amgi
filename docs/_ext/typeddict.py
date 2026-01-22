from __future__ import annotations

import importlib
from collections.abc import Iterable
from collections.abc import Iterator
from types import NoneType
from types import UnionType
from typing import Any
from typing import get_args
from typing import get_origin
from typing import get_type_hints
from typing import Literal
from typing import Optional
from typing import TypeGuard
from typing import Union

from docutils import nodes
from docutils.parsers.rst import Directive
from docutils.statemachine import StringList
from sphinx.addnodes import pending_xref
from sphinx.application import Sphinx
from sphinx.util.typing import ExtensionMetadata

try:
    from typing import is_typeddict
except ImportError:
    is_typeddict = None  # type: ignore[assignment]


def _import_python_object(dotted_path: str) -> Any:
    module_path, _, object_name = dotted_path.rpartition(".")

    if not module_path or not object_name:
        raise ValueError(f"Invalid dotted path: {dotted_path!r}")

    imported_module = importlib.import_module(module_path)
    return getattr(imported_module, object_name)


def _is_typed_dict_class(candidate_object: Any) -> TypeGuard[type]:
    if is_typeddict is not None:
        return bool(is_typeddict(candidate_object))
    return (
        isinstance(candidate_object, type)
        and hasattr(candidate_object, "__annotations__")
        and hasattr(candidate_object, "__required_keys__")
        and hasattr(candidate_object, "__optional_keys__")
    )


def _resolve_type_hints(typed_dict_class: type) -> dict[str, Any]:
    try:
        return get_type_hints(typed_dict_class, include_extras=True)
    except TypeError:
        return get_type_hints(typed_dict_class)


def _definition_order(typed_dict_class: type) -> Iterable[str]:
    return (typed_dict_class.__annotations__ or {}).keys()


def _find_nested_typed_dict(type_hint: Any) -> type | None:
    if _is_typed_dict_class(type_hint):
        return type_hint

    for arg in get_args(type_hint) or ():
        found = _find_nested_typed_dict(arg)
        if found:
            return found

    return None


def _is_literal_origin(origin_object: Any) -> bool:
    if Literal is not None and origin_object is Literal:
        return True

    module_name = getattr(origin_object, "__module__", None)
    qual_name = getattr(origin_object, "__qualname__", None) or getattr(
        origin_object, "__name__", None
    )

    return (module_name, qual_name) in {
        ("typing", "Literal"),
        ("typing_extensions", "Literal"),
    }


def _is_optional_type(origin: Any, args: tuple[Any, ...]) -> bool:
    if origin in (Union, UnionType):
        return NoneType in args
    return False


def _python_obj_ref(obj: Any, label: str) -> nodes.Node:
    if obj.__module__ == "builtins":
        target = obj.__name__
    elif obj.__module__ == "typing_extensions":
        target = f"typing.{obj.__qualname__}"
    else:
        target = f"{obj.__module__}.{obj.__qualname__}"

    return pending_xref(
        "",
        nodes.literal(text=label),
        refdomain="py",
        reftype="obj",
        reftarget=target,
        refexplicit=True,
    )


def _yield_type_nodes(type_hint: Any) -> Iterator[nodes.Node]:
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if origin is None:
        display_name = getattr(type_hint, "__qualname__", None) or getattr(
            type_hint, "__name__", None
        )
        if display_name:
            yield _python_obj_ref(type_hint, display_name)
        else:
            yield nodes.literal(text=repr(type_hint))
        return

    origin_display_name = (
        getattr(origin, "__qualname__", None)
        or getattr(origin, "__name__", None)
        or str(origin)
    )
    if _is_optional_type(origin, args):
        yield _python_obj_ref(Optional, "Optional")
    else:
        yield _python_obj_ref(origin, origin_display_name)
    yield nodes.Text("[")

    if _is_literal_origin(origin):
        for index, literal_value in enumerate(args):
            if index:
                yield nodes.Text(", ")
            yield nodes.literal(text=repr(literal_value))
        yield nodes.Text("]")
        return

    filtered_args = (
        filter(lambda arg: arg is not NoneType, args)
        if _is_optional_type(origin, args)
        else args
    )

    for index, argument in enumerate(filtered_args):
        if index:
            yield nodes.Text(", ")
        yield from _yield_type_nodes(argument)

    yield nodes.Text("]")


def _format_accessor(type_name: str, keys: Iterable[str]) -> str:
    return type_name + "".join(f'["{k}"]' for k in keys)


def _nested_parse_rst(directive: Directive, text: str, source: str) -> list[nodes.Node]:
    if not text.strip():
        return []

    string_list = StringList()
    for line in text.splitlines():
        string_list.append(line, source)

    container = nodes.container()
    directive.state.nested_parse(string_list, directive.content_offset, container)
    return list(container.children)


def _extract_docstring_var_nodes(
    directive: Directive, typed_dict_class: type
) -> dict[str, list[nodes.Node]]:
    doc = typed_dict_class.__doc__ or ""
    if not doc.strip():
        return {}

    source = (
        f"<typeddict-doc:{typed_dict_class.__module__}.{typed_dict_class.__qualname__}>"
    )
    top_nodes = _nested_parse_rst(directive, doc, source)

    vars_nodes: dict[str, list[nodes.Node]] = {}

    for node in top_nodes:
        for field in node.findall(nodes.field):
            field_name_node = field[0]
            field_body_node = field[1]

            parts = field_name_node.astext().strip().split(None, 1)
            if len(parts) != 2:
                continue

            tag, name = parts[0], parts[1].strip()
            if tag != "var" or not name:
                continue

            vars_nodes[name] = list(field_body_node.children)

    return vars_nodes


def _append_doc_nodes_to_list_item(
    list_item: nodes.list_item,
    paragraph: nodes.paragraph,
    doc_nodes: list[nodes.Node],
) -> None:
    if not doc_nodes:
        return

    first, *rest = doc_nodes

    if isinstance(first, nodes.paragraph):
        paragraph += nodes.Text(" – ")
        paragraph.extend(child for child in first.children)
    else:
        rest = doc_nodes

    for extra in rest:
        list_item += extra


def _yield_leaf_items(
    typed_dict_class: type,
    type_name: str,
    parent_keys: tuple[str, ...],
    directive: Directive,
) -> Iterator[nodes.list_item]:
    resolved_hints = _resolve_type_hints(typed_dict_class)

    docstring_var_nodes = _extract_docstring_var_nodes(directive, typed_dict_class)

    for key in _definition_order(typed_dict_class):
        raw_type_hint = resolved_hints.get(key, Any)

        full_path = parent_keys + (key,)
        nested_typed_dict = _find_nested_typed_dict(raw_type_hint)

        if nested_typed_dict is not None:
            yield from _yield_leaf_items(
                typed_dict_class=nested_typed_dict,
                type_name=type_name,
                parent_keys=full_path,
                directive=directive,
            )
            continue

        list_item = nodes.list_item()
        paragraph = nodes.paragraph()

        paragraph += nodes.literal(text=_format_accessor(type_name, full_path))
        paragraph += nodes.Text(" (")

        for type_node in _yield_type_nodes(raw_type_hint):
            paragraph += type_node

        paragraph += nodes.Text(")")

        doc_nodes = docstring_var_nodes.get(key, [])

        list_item += paragraph

        _append_doc_nodes_to_list_item(
            list_item,
            paragraph,
            doc_nodes,
        )

        yield list_item


class TypedDictDirective(Directive):
    has_content = False
    required_arguments = 1
    option_spec = {"type": str}

    def run(self) -> list[nodes.Node]:
        dotted_path = self.arguments[0].strip()
        typed_dict_class = _import_python_object(dotted_path)

        if not _is_typed_dict_class(typed_dict_class):
            return [
                self.state_machine.reporter.error(
                    f"{dotted_path!r} is not a TypedDict",
                    line=self.lineno,
                )
            ]

        type_name = self.options.get("type", "message")

        unordered_list = nodes.bullet_list()
        for list_item in _yield_leaf_items(
            typed_dict_class,
            type_name,
            (),
            self,
        ):
            unordered_list += list_item

        return [unordered_list]


def setup(app: Sphinx) -> ExtensionMetadata:
    app.add_directive("typeddict", TypedDictDirective)
    return {
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
