import re
import logging


class SQLRedactor:
    REDACTION_STRING = 'REDACTED'

    def __init__(self):
        self.redactors = {}
        self.key_equal_value_redactor_keys = ['master_symmetric_key', 'aws_secret_access_key', 'password']
        self.keywords_to_redact_value_from = ['secret_access_key']

    def apply_all_redactions(self, input_string):
        output_string = input_string
        for k in self.key_equal_value_redactor_keys:
            logging.debug('Applying key=value redactor {r}'.format(r=k))
            output_string = SQLRedactor.remove_string_value_from_key_equal_sign_value_pair(k, output_string)
        for k in self.keywords_to_redact_value_from:
            logging.debug('Applying keyword redactor {r}'.format(r=k))
            output_string = SQLRedactor.remove_keyword_value(k, output_string)
        return output_string

    @staticmethod
    def remove_string_value_from_key_equal_sign_value_pair(key_to_redact, input_string):
        output_string = input_string
        for match in re.finditer(r'(' + key_to_redact + '=[^\'; ]+)', input_string.lower()):
            for i in range(0, len(match.groups())):
                string_to_replace = output_string[match.start(i):match.end(i)]
                output_string = output_string.replace(string_to_replace,
                                                      key_to_redact + '=' + SQLRedactor.REDACTION_STRING)
        return output_string

    @staticmethod
    def remove_keyword_value(keyword_to_redact, input_string):
        output_string = input_string
        for match in re.finditer(r'(' + keyword_to_redact + "[ \n]+'[^']*')", input_string.lower()):
            for i in range(0, len(match.groups())):
                string_to_replace = output_string[match.start(i):match.end(i)]
                safe_string = keyword_to_redact + " '" + SQLRedactor.REDACTION_STRING + "'"
                output_string = output_string.replace(string_to_replace, safe_string)
        return output_string

    @staticmethod
    def get_log_safe_string(input_string):
        return SQLRedactor().apply_all_redactions(input_string)

GET_SAFE_LOG_STRING = SQLRedactor.get_log_safe_string


class SQLTextHelper:
    BLOCK_COMMENT_START = '/*'
    BLOCK_COMMENT_END = '*/'
    LINE_COMMENT_START = '--'
    TOKEN_SEPARATOR = '\'"'

    def __init__(self):
        pass

    @staticmethod
    def get_sql_without_block_comments(input_sql_text):
        end_position = -20
        sql_text = input_sql_text

        while end_position == -20:
            start_position = sql_text.find(SQLTextHelper.BLOCK_COMMENT_START)
            if start_position >= 0:
                end_position = sql_text.find(SQLTextHelper.BLOCK_COMMENT_END, start_position)
            else:
                return sql_text

            if end_position > start_position:
                sql_text = sql_text[0:start_position] + sql_text[end_position+len(SQLTextHelper.BLOCK_COMMENT_END):]
                end_position = -20
        return sql_text

    @staticmethod
    def remove_line_comments_when_no_block_comments_are_present(input_sql_text):
        is_in_token = None
        is_in_single_line_comment = False
        last_letter = 'a'
        result_sql_text = ''

        for letter in input_sql_text:
            if is_in_single_line_comment:
                if letter == '\n':
                    is_in_single_line_comment = False
                else:
                    continue
            if is_in_token is not None:
                result_sql_text += letter
            else:
                if not (last_letter == '-' and letter == '-'):
                    result_sql_text += letter
                else:
                    is_in_single_line_comment = True
                    result_sql_text = result_sql_text[:-1]
            if letter in SQLTextHelper.TOKEN_SEPARATOR:
                if is_in_token is None:
                    is_in_token = letter
                else:
                    if letter == is_in_token:
                        is_in_token = None
            last_letter = letter

        return SQLTextHelper.remove_empty_lines(result_sql_text)

    @staticmethod
    def remove_empty_lines(string):
        lines = string.split('\n')
        return '\n'.join([line for line in lines if len(line) > 0])

    @staticmethod
    def get_sql_without_comments(input_sql_text):
        return SQLTextHelper.remove_line_comments_when_no_block_comments_are_present(
            SQLTextHelper.get_sql_without_block_comments(input_sql_text)
        )

    @staticmethod
    def get_sql_without_commands_newlines_and_whitespace(input_sql_text):
        sql_text = SQLTextHelper.get_sql_without_comments(input_sql_text).\
            replace('\n', ' ').\
            replace('\r', ' ').\
            replace('\t', '   ')
        is_in_token = None
        last_letter = 'a'
        result_sql_text = ''
        for letter in sql_text:
            if is_in_token is not None:
                result_sql_text += letter
            else:
                if not (last_letter == ' ' and letter == ' '):
                    result_sql_text += letter
            if letter in SQLTextHelper.TOKEN_SEPARATOR:
                if is_in_token is None:
                    is_in_token = letter
                else:
                    if letter == is_in_token:
                        is_in_token = None
            last_letter = letter
        return result_sql_text.strip()

    @staticmethod
    def remove_trailing_semicolon(sql_text):
        sql_text = sql_text.rstrip()
        if sql_text.endswith(';'):
            sql_text = sql_text[:-1]
        return sql_text

    @staticmethod
    def quote_indent(identifier_string):
        if identifier_string.startswith('"') and identifier_string.endswith('"'):
            logging.warning('QUOTE_INDENT() called on identifier already surrounded by double quotes {id}'.format(
                id=identifier_string
            ))
        if '"' in identifier_string:
            return '"{string}"'.format(string=identifier_string.replace('"', '""'))
        else:
            return identifier_string

    @staticmethod
    def quote_unindent(identifier_string):
        if identifier_string.startswith('"') and identifier_string.endswith('"'):
            identifier_string = identifier_string[1:-1]
            return identifier_string.replace('""', '"')
        if '"' in identifier_string:
            raise ValueError('quote_unindent cannot have a value with double quotes if not surrounded by double quotes')
        return identifier_string

    @staticmethod
    def get_first_double_quoted_identifier(string):
        if '"' not in string:
            raise ValueError('No double quote in {s} so cannot get first double quoted identifier.'.format(s=string))
        start_pos = string.find('"')
        pos = start_pos + 1
        while pos < len(string):
            if string[pos] == '"' and pos + 1 < len(string) and string[pos+1] == '"':
                pos += 2
            elif string[pos] == '"' and (pos + 1 == len(string) or string[pos+1] != '"'):
                break
            else:
                pos += 1
        return string[start_pos:pos + 1]
