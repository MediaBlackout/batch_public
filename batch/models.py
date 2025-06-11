"""Model selection helper.

This indirection exists so the rest of the codebase can refer to the logical
size of a model (nano/mini/full) instead of the exact model identifier which
will almost certainly change over time.

Models with Batch Support
Text / Chat Models:
- gpt-4.1-2025-04-14
- gpt-4.1-mini-2025-04-14
- gpt-4.1-nano-2025-04-14
- gpt-4o
- gpt-4o-mini
- gpt-4o-2024-05-13
- gpt-3.5-turbo
- gpt-3.5-turbo-16k
- gpt-4
- gpt-4-32k
- gpt-4-turbo-preview
- gpt-4-vision-preview
- gpt-4-turbo
- gpt-4-0125-preview
- gpt-3.5-turbo-1106
- gpt-4-0314
- gpt-4-turbo-2024-04-09
- gpt-4-32k-0314
- gpt-4-32k-0613

Embedding Models:
- text-embedding-3-large
- text-embedding-3-small
- text-embedding-ada-002

The Batch API processes asynchronous jobs (text, chat, image) and returns them within 24 hours.
"""


MODEL_MAP = {
    "nano": "gpt-4.1-nano-2025-04-14",
    "mini": "gpt-4.1-mini-2025-04-14",
    "full": "gpt-4.1-2025-04-14",
}

TEXT_CHAT_MODELS = [
    "gpt-4.1-2025-04-14",
    "gpt-4.1-mini-2025-04-14",
    "gpt-4.1-nano-2025-04-14",
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4o-2024-05-13",
    "gpt-3.5-turbo",
    "gpt-3.5-turbo-16k",
    "gpt-4",
    "gpt-4-32k",
    "gpt-4-turbo-preview",
    "gpt-4-vision-preview",
    "gpt-4-turbo",
    "gpt-4-0125-preview",
    "gpt-3.5-turbo-1106",
    "gpt-4-0314",
    "gpt-4-turbo-2024-04-09",
    "gpt-4-32k-0314",
    "gpt-4-32k-0613",
]

EMBEDDING_MODELS = [
    "text-embedding-3-large",
    "text-embedding-3-small",
    "text-embedding-ada-002",
]

SUPPORTED_MODELS = TEXT_CHAT_MODELS + EMBEDDING_MODELS


def resolve(model_key: str) -> str:
    """Return the concrete model name for *model_key*.

    If *model_key* is a logical key (nano/mini/full), returns the mapped model.
    If it matches a known supported model name, returns it directly.
    Defaults to the nano model if an unknown key is supplied.
    """

    if model_key in MODEL_MAP:
        return MODEL_MAP[model_key]
    if model_key in SUPPORTED_MODELS:
        return model_key
    return MODEL_MAP["nano"]
