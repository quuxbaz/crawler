import publicsuffix
import urlparse

PSL = None

def domain(url):
    global PSL
    if not PSL:
        PSL = publicsuffix.PublicSuffixList(publicsuffix.fetch())

    u = urlparse.urlparse(url)
    d = PSL.get_public_suffix(u.netloc)
    return d

def json_deep_get(js, key, default = ""):
    """
    JSON [Key] String -> String
    It takes a json JS, a list [key1, key2, ..., keyN] and produces
    the value JS[key1][key2]...[keyN] if it exists.  If it doesn't
    exist, produce the default parameter.

    Examples: 
    json_deep_get({}, [], "hello") = ValueError()
    json_deep_get({"x": {1: {"z": "jesus"}}}, ['x', 1, 'z'], "hello") = "jesus"
    json_deep_get({"x": {1: {"z": "jesus"}}}, ['y'], "hello") = "hello"
    """

    if key == []:
        raise ValueError("key list cannot be empty")

    if key[0] not in js:
        return default
    elif not isinstance(js, dict):
        raise TypeError
    else:
        if len(key) == 1:
            return js[key[0]]
        else:
            return json_deep_get(js[key[0]], key[1:], default)
