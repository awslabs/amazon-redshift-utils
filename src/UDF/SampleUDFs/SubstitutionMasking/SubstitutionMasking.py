#!/usr/bin/env python

# generate a simple ciphertext key which will be used for subsequent simple substitution operations
def generateCiphertextKey():
    import random
        
    # alphabet as charlist 
    l = list('abcdefghijklmnopqrstuvwxyz')
    
    # shuffle charlist
    random.shuffle(l)
    
    # return new string from shuffled charlist
    return ''.join(l)

# encipher the given value with the supplied ciphertext key
def simpleEncipher(value, withKey):
    from pycipher import SimpleSubstitution
    
    ss = SimpleSubstitution(withKey)
    return ss.encipher(str(value), True)[::-1]

# decipher the given enciphered value using the same key as input
def simpleDecipher(value, withKey):
    from pycipher import SimpleSubstitution
    
    ss = SimpleSubstitution(withKey)
    return ss.decipher(str(value)[::-1], True)

# encipher the given value with the specified multiplicative and additive portions
# using an Affine cipher
def affineEncipher(value, mult=5, add=9):
    from pycipher import Affine
    
    af = Affine(mult, add)
    return af.encipher(str(value), True)[::-1]

# decipher the given enciphered value with the specified multiplicative and additive 
# portions using an Affine cipher
def affineDecipher(value, mult=5, add=9):
    from pycipher import Affine
    
    af = Affine(mult, add)
    return af.decipher(str(value)[::-1], True)
    
