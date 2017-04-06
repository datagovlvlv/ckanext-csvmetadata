from pylons import config
from pylons.i18n import gettext

def lang():
    # access this function late in case ckan
    # is not set up fully when importing this module
    from ckantoolkit import h
    return h.lang()

def csvmetadata_language_text(text, prefer_lang=None):
    """
    :param text: {lang: text} dict or text string
    :param prefer_lang: choose this language version if available
    Convert "language-text" to users' language by looking up
    languag in dict or using gettext if not a dict
    """
    if not text:
        return u''

    if hasattr(text, 'get'):
        try:
            if prefer_lang is None:
                prefer_lang = lang()
        except TypeError:
            pass  # lang() call will fail when no user language available
        else:
            try:
                return text[prefer_lang]
            except KeyError:
                pass

        default_locale = config.get('ckan.locale_default', 'en')
        try:
            return text[default_locale]
        except KeyError:
            pass

        l, v = sorted(text.items())[0]
        return v

    t = gettext(text)
    if isinstance(t, str):
        return t.decode('utf-8')
    return t
