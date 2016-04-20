""" Concise UUID generation. """

import binascii
import math
import os
import uuid as _uu


class ShortUUID(object):
    def __init__(self, alphabet=None):
        if alphabet is None:
            alphabet = list("23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
                            "abcdefghijkmnopqrstuvwxyz")

        self.set_alphabet(alphabet)

    @property
    def _length(self):
        """
        Return the necessary length to fit the entire UUID given
        the current alphabet.
        """
        return int(math.ceil(math.log(2 ** 128, self._alpha_len)))

    def _num_to_string(self, number, pad_to_length=None):
        """
        Convert a number to a string, using the given alphabet.
        """
        output = ""
        while number:
            number, digit = divmod(number, self._alpha_len)
            output += self._alphabet[digit]
        if pad_to_length:
            remainder = max(pad_to_length - len(output), 0)
            output = output + self._alphabet[0] * remainder
        return output

    def _string_to_int(self, string):
        """
        Convert a string to a number, using the given alphabet..
        """
        number = 0
        for char in string[::-1]:
            number = number * self._alpha_len + self._alphabet.index(char)
        return number

    def encode(self, uuid, pad_length=None):
        """
        Encodes a UUID into a string (LSB first) according to the alphabet
        If leftmost (MSB) bits 0, string might be shorter
        """
        if pad_length is None:
            pad_length = self._length
        return self._num_to_string(uuid.int, pad_to_length=pad_length)

    def decode(self, string):
        """
        Decodes a string according to the current alphabet into a UUID
        Raises ValueError when encountering illegal characters
        or too long string
        If string too short, fills leftmost (MSB) bits with 0.
        """
        return _uu.UUID(int=self._string_to_int(string))

    def uuid(self, name=None, pad_length=None):
        """
        Generate and return a UUID.

        If the name parameter is provided, set the namespace to the provided
        name and generate a UUID.
        """
        if pad_length is None:
            pad_length = self._length

        # If no name is given, generate a random UUID.
        if name is None:
            uuid = _uu.uuid4()
        elif "http" not in name.lower():
            uuid = _uu.uuid5(_uu.NAMESPACE_DNS, name)
        else:
            uuid = _uu.uuid5(_uu.NAMESPACE_URL, name)
        return self.encode(uuid, pad_length)

    def random(self, length=None):
        """
        Generate and return a cryptographically-secure short random string
        of the specified length.
        """
        if length is None:
            length = self._length

        random_num = int(binascii.b2a_hex(os.urandom(length)), 16)
        return self._num_to_string(random_num, pad_to_length=length)[:length]

    def get_alphabet(self):
        """Return the current alphabet used for new UUIDs."""
        return ''.join(self._alphabet)

    def set_alphabet(self, alphabet):
        """Set the alphabet to be used for new UUIDs."""

        # Turn the alphabet into a set and sort it to prevent duplicates
        # and ensure reproducibility.
        new_alphabet = list(sorted(set(alphabet)))
        if len(new_alphabet) > 1:
            self._alphabet = new_alphabet
            self._alpha_len = len(self._alphabet)
        else:
            raise ValueError("Alphabet with more than "
                             "one unique symbols required.")

    def encoded_length(self, num_bytes=16):
        """
        Returns the string length of the shortened UUID.
        """
        factor = math.log(256) / math.log(self._alpha_len)
        return int(math.ceil(factor * num_bytes))


# For backwards compatibility
_global_instance = ShortUUID()
encode = _global_instance.encode
decode = _global_instance.decode
uuid = _global_instance.uuid
random = _global_instance.random
get_alphabet = _global_instance.get_alphabet
set_alphabet = _global_instance.set_alphabet
