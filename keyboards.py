from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder


def subscribe_keyboard(channel_url: str, channel_id: str) -> InlineKeyboardMarkup:
    """Кнопки для подписки на канал."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="📢 Подписаться на канал",
            url=channel_url
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="✅ Я подписался",
            callback_data="check_sub"
        )
    )
    return builder.as_markup()


def check_again_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="🔄 Проверить снова",
            callback_data="check_sub"
        )
    )
    return builder.as_markup()
