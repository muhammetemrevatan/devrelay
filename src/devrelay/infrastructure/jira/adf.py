from pydantic import JsonValue

from devrelay.application.ports.task_source import InvalidTaskSourceData


def _invalid() -> InvalidTaskSourceData:
    return InvalidTaskSourceData(
        "Jira document contains invalid or unsupported content."
    )


def _object(value: JsonValue) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise _invalid()
    return value


def _children(node: dict[str, JsonValue]) -> list[JsonValue]:
    children = node.get("content", [])
    if not isinstance(children, list):
        raise _invalid()
    return children


def _attribute(node: dict[str, JsonValue], name: str) -> str:
    value = _object(node.get("attrs")).get(name)
    if not isinstance(value, str):
        raise _invalid()
    return value


def _render(value: JsonValue, depth: int, budget: list[int]) -> str:
    budget[0] -= 1
    if depth > 32 or budget[0] < 0:
        raise InvalidTaskSourceData("Jira document exceeds structural limits.")
    node = _object(value)
    kind = node.get("type")
    if not isinstance(kind, str):
        raise _invalid()
    if kind in {
        "text",
        "hardBreak",
        "rule",
        "mention",
        "emoji",
        "status",
        "inlineCard",
    }:
        if "content" in node:
            raise _invalid()
    if kind == "text":
        text = node.get("text")
        if not isinstance(text, str) or len(text) > 65_536:
            raise _invalid()
        marks = node.get("marks", [])
        if not isinstance(marks, list):
            raise _invalid()
        for raw in marks:
            mark = _object(raw)
            if not isinstance(mark.get("type"), str):
                raise _invalid()
            if mark.get("type") == "link":
                href = _attribute(mark, "href")
                if text != href:
                    text = f"{text} ({href})"
            elif mark.get("type") == "strike":
                text = f"~~{text}~~"
            elif mark.get("type") not in {
                "strong",
                "em",
                "code",
                "strike",
                "underline",
                "subsup",
            }:
                raise _invalid()
        return text
    if kind == "hardBreak":
        return "\n"
    if kind == "rule":
        return "---"
    if kind in {"mention", "emoji", "status"}:
        return _attribute(node, "shortName" if kind == "emoji" else "text")
    if kind == "inlineCard":
        return _attribute(node, "url")
    if kind not in {
        "doc",
        "paragraph",
        "heading",
        "blockquote",
        "codeBlock",
        "listItem",
        "bulletList",
        "orderedList",
        "table",
        "tableRow",
        "tableHeader",
        "tableCell",
    }:
        raise _invalid()
    children = _children(node)
    inline = {"text", "hardBreak", "mention", "emoji", "inlineCard", "status"}
    blocks = {
        "paragraph",
        "heading",
        "blockquote",
        "codeBlock",
        "bulletList",
        "orderedList",
        "table",
        "rule",
    }
    allowed_children = {
        "doc": blocks,
        "paragraph": inline,
        "heading": inline,
        "codeBlock": {"text"},
        "table": {"tableRow"},
        "tableRow": {"tableCell", "tableHeader"},
        "bulletList": {"listItem"},
        "orderedList": {"listItem"},
        "listItem": blocks - {"table", "heading", "rule"},
        "blockquote": blocks,
        "tableCell": blocks - {"table", "rule"},
        "tableHeader": blocks - {"table", "rule"},
    }
    for child in children:
        child_type = _object(child).get("type")
        if not isinstance(child_type, str) or child_type not in allowed_children[kind]:
            raise _invalid()
    rendered = [_render(child, depth + 1, budget) for child in children]
    if kind in {"bulletList", "orderedList"}:
        if any(_object(child).get("type") != "listItem" for child in children):
            raise _invalid()
        start = 1
        if kind == "orderedList":
            start_value = _object(node.get("attrs", {})).get("order", 1)
            if type(start_value) is not int or start_value < 1:
                raise _invalid()
            start = start_value
        return "\n".join(
            f"{'-' if kind == 'bulletList' else str(index) + '.'} {text}"
            for index, text in enumerate(rendered, start=start)
        )
    separator = ""
    if kind in {"doc", "blockquote", "listItem", "table", "tableCell", "tableHeader"}:
        separator = "\n"
    elif kind == "tableRow":
        separator = " | "
    text = separator.join(rendered)
    if len(text) > 65_536:
        raise InvalidTaskSourceData("Jira document exceeds text limits.")
    return text


def _document(value: JsonValue) -> dict[str, JsonValue]:
    node = _object(value)
    if node.get("type") != "doc" or type(node.get("version")) is not int:
        raise _invalid()
    if node.get("version") != 1 or "content" not in node:
        raise _invalid()
    return node


def render_document(value: JsonValue) -> str:
    if value is None:
        return ""
    node = _document(value)
    return _render(node, 0, [4_000])


def _list_items(node: dict[str, JsonValue]) -> tuple[str, ...]:
    return tuple(_render(item, 0, [4_000]) for item in _children(node))


def description_criteria(value: JsonValue) -> tuple[str, ...]:
    """Only the list immediately following an explicit criteria section counts."""
    if value is None:
        return ()
    children = _children(_document(value))
    found: tuple[str, ...] | None = None
    for index, raw in enumerate(children):
        node = _object(raw)
        if node.get("type") not in {"paragraph", "heading"}:
            continue
        title = _render(node, 0, [4_000]).strip().removesuffix(":").strip().casefold()
        if title != "acceptance criteria":
            continue
        if found is not None:
            raise InvalidTaskSourceData("Jira description contains ambiguous criteria.")
        if index + 1 >= len(children):
            raise InvalidTaskSourceData(
                "Jira criteria section must be followed by a list."
            )
        following = _object(children[index + 1])
        if following.get("type") not in {"bulletList", "orderedList"}:
            raise InvalidTaskSourceData(
                "Jira criteria section must be followed by a list."
            )
        found = _list_items(following)
    return found or ()


def custom_field_criteria(value: JsonValue) -> tuple[str, ...]:
    """Configured text lines, string arrays or ADF blocks are explicit criteria."""
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(line.strip() for line in value.splitlines() if line.strip())
    if isinstance(value, list):
        if not all(isinstance(item, str) for item in value):
            raise _invalid()
        return tuple(item for item in value if isinstance(item, str))
    render_document(value)
    result: list[str] = []
    for raw in _children(_document(value)):
        node = _object(raw)
        if node.get("type") in {"bulletList", "orderedList"}:
            result.extend(_list_items(node))
        else:
            result.append(_render(node, 0, [4_000]))
    return tuple(result)
