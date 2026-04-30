/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar Regex;

regex
    : sequence EOF
    ;

sequence
    : quantified+
    ;

quantified
    : atom quantifier?
    ;

quantifier
    : QUESTION_MARK
    | STAR
    | PLUS
    // TODO (https://starburstdata.atlassian.net/browse/ENG-12564) support lazy and eager quantifiers
    ;

atom
    : literal
    | DOT
    | anchor
    | escape
    | group
    | charClass
    ;

literal
    : LITERAL_CHAR
    | DIGIT
    | COLON
    | HYPHEN
    ;

anchor
    : CARET
    | DOLLAR
    ;

escape
    : ESCAPED_META
    ;

group
    : OPEN_PAREN QUESTION_MARK COLON sequence CLOSE_PAREN   # nonCapturingGroup
    | OPEN_PAREN sequence CLOSE_PAREN                       # capturingGroup
    ;

charClass
    : OPEN_BRACKET CARET charClassBody CLOSE_BRACKET   # negatedCharClass
    | OPEN_BRACKET charClassBody CLOSE_BRACKET         # positiveCharClass
    ;

charClassBody
    : charClassAtom+
    ;

charClassAtom
    : charClassLiteral (HYPHEN charClassLiteral)?   # charClassLiteralOrRange
    | charClassEscape                               # charClassEscapeAtom
    ;

charClassLiteral
    : LITERAL_CHAR
    | DIGIT
    | DOT
    | QUESTION_MARK
    | STAR
    | PLUS
    | OPEN_PAREN
    | CLOSE_PAREN
    | DOLLAR
    | COLON
    | HYPHEN
    ;

charClassEscape
    : ESCAPED_META
    ;

CARET           : '^';
CLOSE_BRACKET   : ']';
CLOSE_PAREN     : ')';
COLON           : ':';
DIGIT           : [0-9];
DOLLAR          : '$';
DOT             : '.';
// Only metacharacter escapes are recognized. Unknown escapes (e.g. \n, \d, \w)
// cause a lexer error, which surfaces as a ParsingException and falls back to CPU.
ESCAPED_META    : '\\' [\\^$.|?*+(){}[\]];
HYPHEN          : '-';
LITERAL_CHAR    : ~[\\^$.|?*+(){}[\]\-0-9:];
OPEN_BRACKET    : '[';
OPEN_PAREN      : '(';
PLUS            : '+';
QUESTION_MARK   : '?';
STAR            : '*';
