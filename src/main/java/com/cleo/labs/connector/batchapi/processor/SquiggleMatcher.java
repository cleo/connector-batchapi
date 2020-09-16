package com.cleo.labs.connector.batchapi.processor;

import java.util.regex.Matcher;

/**
 * An engine designed to match embedded expression sequences in an input string
 * with the intention of evaluating and replacing them. It follows the pattern
 * of {@link #find()}, {@link #appendReplacement(StringBuilder, String)} and
 * {@link #appendTail(StringBuilder)} methods described at
 * {@link Matcher#appendReplacement(StringBuffer, String)}, although using the
 * more modern {@link StringBuilder} class instead of {@link StringBuffer}.
 * <p/>
 * Embedded expressions are enclosed in
 * <span style="font-family: monospace">${expression}</span> delimiters. The
 * rules for identifying an expression are biased towards JavaScript expressions
 * when searching for the closing brace
 * <span style="font-family: monospace">}</span>:
 * <ul><li>braces inside strings are ignored</li>
 *     <li>strings may be delimited by single {@code '} or double quotes {@code "}</li>
 *     <li>backslash may be used to escape characters, including ' and ", inside strings</li>
 *     <li>nested unquoted brace pairs {} are respected</li>
 * </ul>
 * </p>
 * Sample usage:
 * 
 * <pre>
 * SquiggleMatcher m = new SquiggleMatcher(input);
 * StringBuilder sb = new StringBuilder();
 * while (m.find()) {
 *   m.appendReplacement(sb, evaluate(m.expression()));
 * }
 * m.appendTail(sb);
 * return sb.toString();
 * </pre>
 */
public class SquiggleMatcher {

  private CharSequence input;
  private int mark;
  private int start;
  private int end;
  private boolean found;

  /**
   * Create a new {@code SquiggleMatcher} over the supplied {@code input}. A
   * {@code null} input is treated as an empty string.
   * 
   * @param input
   *          the {@code CharSequence} to process
   */
  public SquiggleMatcher(CharSequence input) {
    this.input = input == null ? "" : input;
    this.mark = 0;
    this.start = 0;
    this.end = 0;
    this.found = false;
  }

  /**
   * Analyzes the input to see if it consists of a single
   * embedded ${expression} and nothing else: no prefix,
   * no suffix. Returns "expression" for a singleton,
   * otherwise {@code null}.
   * @return the "expression" or {@code null}
   */
  public String singleton() {
      SquiggleMatcher tester = new SquiggleMatcher(input);
      if (tester.find() && tester.start == 0 && tester.end == input.length()) {
          return tester.expression();
      } else {
          return null;
      }
  }

  /**
   * Searches for the next embedded
   * <span style="font-family: monospaced">${expression}</span> in the input,
   * returning {@code true} and isolating the {@code expression} for retrieval
   * with {@link #expression()} if one is found, or returning {@code false} if
   * no more embedded expressions are found (but don't forget to call
   * {@link #appendReplacement(StringBuilder, String)}).
   * 
   * @return whether an embedded expression was found or not
   */
  public boolean find() {
    mark = end;
    int i = end;
    char mode = ' ';
    int nesting = 0;
    while (i < input.length()) {
      char c = input.charAt(i);
      switch (mode) {
      case ' ':
        if (c == '$') {
          mode = '$';
          start = i;
        }
        break;
      case '$':
        if (c == '{') {
          mode = '{';
        } else {
          mode = ' ';
        }
        break;
      case '{':
        if (c == '{') {
          nesting++;
        } else if (c == '}') {
          if (nesting == 0) {
            end = i + 1;
            found = true;
            return true;
          } else {
            nesting--;
          }
        } else if (c == '\'' || c == '"') {
          mode = c;
        }
        break;
      case '\'':
      case '"':
        if (c == '\\') {
          i++;
        } else if (c == mode) {
          mode = '{';
        }
        break;
      default:
      }
      i++;
    }
    found = false;
    return false;
  }

  /**
   * If {@link #find()} returned {@code true}, returns the captured embedded
   * expression (which may be empty). Otherwise returns an empty string by
   * default. Never returns {@code null}.
   * 
   * @return the captured embedded expression, or the empty string
   */
  public String expression() {
    if (found) {
      return input.subSequence(start+2, end-1).toString();
    } else {
      return "";
    }
  }

  /**
   * Appends the next chunk of the input sequence separating the previous
   * matched expression from the most recent one, followed by the specified
   * replacement text. {@link #find()} should be called next, or the behavior is
   * unpredictable.
   * 
   * @param sb
   *          the {@link StringBuilder} to which text should be appended
   * @param replacement
   *          the text to replace the last matched expression
   * @return {@code this} SquiggleMatcher object
   */
  public SquiggleMatcher appendReplacement(StringBuilder sb, String replacement) {
    sb.append(input.subSequence(mark, start))
        .append(replacement);
    return this;
  }

  /**
   * After {@link #find()} has returned {@code false}, appends the remaining
   * unmatched text from the input sequence.
   * 
   * @param sb
   *          the {@link StringBuilder} to which text should be appended
   * @return {@code this} SquiggleMatcher object
   */
  public StringBuilder appendTail(StringBuilder sb) {
    return sb.append(input.subSequence(mark, input.length()));
  }
}
