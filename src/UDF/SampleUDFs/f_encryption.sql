/* f_encryption.sql

Purpose: f_encrypt_str encrypts a 255 character string with AES256 encryption using a 32bit key.
f_decrypt_str decrypts this encrypted string and returns the original plaintext.  

Internal dependencies: None

External dependencies: pyaes (https://github.com/ricmoo/pyaes)

2015-09-10: written by chriz@  
*/

CREATE LIBRARY pyaes
language plpythonu 
from 'https://s3.amazonaws.com/casebucket/pyaes.zip';

create or replace function f_encrypt_str(a varchar(255),key char(32))
returns varchar(max)
STABLE
AS $$
    import pyaes
    aes = pyaes.AESModeOfOperationCTR(key)
    e = repr(aes.encrypt(a))
    return e.replace('\\x00','*NUL*')
$$ LANGUAGE plpythonu;

create or replace function f_decrypt_str(a varchar(max),key char(32))
returns varchar(max)
STABLE
AS $$
    import pyaes
    aes = pyaes.AESModeOfOperationCTR(key)
    b = a.replace('*NUL*','\\x00')
    c = eval(b)
    d = aes.decrypt(c)
    return d
$$ LANGUAGE plpythonu;

/* Example usage:

udf=# create table base (c_comment varchar);   
CREATE TABLE

udf=# insert into base values
udf-# ('deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov'),
udf-# ('ainst the ironic, express theodolites. express, even pinto beans among the exp'),
udf-# ('ckages. requests sleep slyly. quickly even pinto beans promise above the slyly regular pinto beans.'),  
udf-# ('platelets. regular deposits detect asymptotes. blithely unusual packages nag slyly at the fluf'),  
udf-# ('nag. furiously careful packages are slyly at the accounts. furiously regular in');
INSERT 0 5

udf=# create table encrypted as select c_comment, f_encrypt_str(c_comment,'PassphrasePassphrasePassphrase32') as c_comment_encrypted from base;
SELECT
udf=# create table decrypted as select c_comment, f_decrypt_str(c_comment_encrypted,'PassphrasePassphrasePassphrase32') as c_comment_decrypted from encrypted;
SELECT
udf=# select * from decrypted;
                                               c_comment                                               |                                          c_comment_decrypted                                          
-------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------
 nag. furiously careful packages are slyly at the accounts. furiously regular in                       | nag. furiously careful packages are slyly at the accounts. furiously regular in
 ckages. requests sleep slyly. quickly even pinto beans promise above the slyly regular pinto beans.   | ckages. requests sleep slyly. quickly even pinto beans promise above the slyly regular pinto beans.
 platelets. regular deposits detect asymptotes. blithely unusual packages nag slyly at the fluf        | platelets. regular deposits detect asymptotes. blithely unusual packages nag slyly at the fluf
 deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov | deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov
 ainst the ironic, express theodolites. express, even pinto beans among the exp                        | ainst the ironic, express theodolites. express, even pinto beans among the exp
(5 rows)

udf=# create table decryption_failure as select c_comment, f_decrypt_str(c_comment_encrypted,'PassphrasePassphrasePassphrase12') as c_comment_decrypted from encrypted;
SELECT
udf=# select * from decryption_failure ;
                                               c_comment                                               |                                          c_comment_decrypted                                          
-------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------
 deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov | MC\x1FZ??9??U$G\x04-T^\x17N????2-\r\x06^\x1F`Ap.??\x11)??i\x0C\x0F\x1B:r\x03uKM3VD\x12KZ%^\x12??Pd?P\x07\x02
 ainst the ironic, express theodolites. express, even pinto beans among the exp                        | HO\x01F?????v\x0CaV\x0F7DRCC??5*\x0CEA     =Tt.\x10%+\r\x0B??V8\x7FE7Z
 ckages. requests sleep slyly. quickly even pinto beans promise above the slyly regular pinto beans.   | JM\x0ER??|_hKJ7[\x0B[R?5\x06
 nag. furiously careful packages are slyly at the accounts. furiously regular in                       | GG\x08\x1B??u??^aHJ4V\x11\J??\u008A.;\x0C\x1C\x11\x18n\x11c(
                                                                                                       : "/\x1D\x18WvlXv\x02K
 platelets. regular deposits detect asymptotes. blithely unusual packages nag slyly at the fluf        | YJ\x0EA|??^$J\x057^\x06D\x0B??(~\x0C\x08^ =]k?^$??:\x1D\x0BX7x\rv\x02I/VD\x12OX`\x16\x16??
(5 rows)

*/
