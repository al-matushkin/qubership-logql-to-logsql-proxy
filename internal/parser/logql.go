package parser

import (
	"fmt"
	"strings"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Tokens
// ────────────────────────────────────────────────────────────────────────────

type tokenType int

const (
	tokILLEGAL  tokenType = iota
	tokEOF                // end of input
	tokLBRACE             // {
	tokRBRACE             // }
	tokLPAREN             // (
	tokRPAREN             // )
	tokLBRACKET           // [
	tokRBRACKET           // ]
	tokCOMMA              // ,
	tokIDENT              // identifier / keyword
	tokSTRING             // "double-quoted string" (value is the unquoted content)
	tokEQ                 // =
	tokNEQ                // !=
	tokRE                 // =~
	tokNRE                // !~
	tokPIPE_EQ            // |=  (line-filter contains)
	tokPIPE_RE            // |~  (line-filter regex)
	tokPIPE               // |
	tokDURATION           // e.g. 5m, 1h30m
)

type token struct {
	typ tokenType
	val string
	pos int // byte offset in source
}

func (t token) String() string { return fmt.Sprintf("%q@%d", t.val, t.pos) }

// ────────────────────────────────────────────────────────────────────────────
// Lexer
// ────────────────────────────────────────────────────────────────────────────

type lexer struct {
	src string
	pos int
}

func (l *lexer) skipSpace() {
	for l.pos < len(l.src) {
		switch l.src[l.pos] {
		case ' ', '\t', '\r', '\n':
			l.pos++
		default:
			return
		}
	}
}

func (l *lexer) next() token {
	l.skipSpace()
	if l.pos >= len(l.src) {
		return token{typ: tokEOF, pos: l.pos}
	}

	pos := l.pos
	ch := l.src[l.pos]

	switch ch {
	case '{':
		l.pos++
		return token{tokLBRACE, "{", pos}
	case '}':
		l.pos++
		return token{tokRBRACE, "}", pos}
	case '(':
		l.pos++
		return token{tokLPAREN, "(", pos}
	case ')':
		l.pos++
		return token{tokRPAREN, ")", pos}
	case '[':
		l.pos++
		return token{tokLBRACKET, "[", pos}
	case ']':
		l.pos++
		return token{tokRBRACKET, "]", pos}
	case ',':
		l.pos++
		return token{tokCOMMA, ",", pos}

	case '=':
		if l.peek1() == '~' {
			l.pos += 2
			return token{tokRE, "=~", pos}
		}
		l.pos++
		return token{tokEQ, "=", pos}

	case '!':
		switch l.peek1() {
		case '=':
			l.pos += 2
			return token{tokNEQ, "!=", pos}
		case '~':
			l.pos += 2
			return token{tokNRE, "!~", pos}
		}
		l.pos++
		return token{tokILLEGAL, "!", pos}

	case '|':
		switch l.peek1() {
		case '=':
			l.pos += 2
			return token{tokPIPE_EQ, "|=", pos}
		case '~':
			l.pos += 2
			return token{tokPIPE_RE, "|~", pos}
		}
		l.pos++
		return token{tokPIPE, "|", pos}

	case '"':
		return l.lexString(pos)

	case '`':
		return l.lexRawString(pos)

	default:
		if isLetter(ch) || ch == '_' {
			return l.lexIdent(pos)
		}
		if isDigit(ch) {
			return l.lexDuration(pos)
		}
		l.pos++
		return token{tokILLEGAL, string(ch), pos}
	}
}

// peek1 returns the byte one position ahead without advancing, or 0.
func (l *lexer) peek1() byte {
	if l.pos+1 < len(l.src) {
		return l.src[l.pos+1]
	}
	return 0
}

// lexString scans a double-quoted string, handling backslash escapes.
// The token value is the unquoted, unescaped content.
func (l *lexer) lexString(pos int) token {
	l.pos++ // skip opening "
	var sb strings.Builder
	for l.pos < len(l.src) {
		ch := l.src[l.pos]
		if ch == '"' {
			l.pos++ // skip closing "
			return token{tokSTRING, sb.String(), pos}
		}
		if ch == '\\' && l.pos+1 < len(l.src) {
			l.pos++
			switch l.src[l.pos] {
			case '"':
				sb.WriteByte('"')
			case '\\':
				sb.WriteByte('\\')
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			default:
				// Pass through unknown escapes verbatim (e.g. \d in regex)
				sb.WriteByte('\\')
				sb.WriteByte(l.src[l.pos])
			}
			l.pos++
			continue
		}
		sb.WriteByte(ch)
		l.pos++
	}
	// Unterminated string
	return token{tokILLEGAL, sb.String(), pos}
}

// lexRawString scans a backtick-quoted raw string literal. Raw strings have no
// escape sequences: every byte between the opening and closing backtick is
// taken literally, matching LogQL's raw-string semantics (identical to Go's).
func (l *lexer) lexRawString(pos int) token {
	l.pos++ // skip opening `
	start := l.pos
	for l.pos < len(l.src) {
		if l.src[l.pos] == '`' {
			val := l.src[start:l.pos]
			l.pos++ // skip closing `
			return token{tokSTRING, val, pos}
		}
		l.pos++
	}
	// Unterminated raw string
	return token{tokILLEGAL, l.src[start:l.pos], pos}
}

// lexIdent scans an identifier (label names, keyword names like "json", "rate").
// In addition to letters, digits, and underscores, label names in LogQL label
// filter stages and "by" clauses may contain dots ('.'), slashes ('/'), and
// hyphens ('-') — as seen with Kubernetes labels such as
// "labels.app.kubernetes.io/name". These characters are valid inside an
// identifier (not as the first character).
func (l *lexer) lexIdent(pos int) token {
	start := l.pos
	for l.pos < len(l.src) {
		ch := l.src[l.pos]
		if isLetter(ch) || isDigit(ch) || ch == '_' || ch == '.' || ch == '/' || ch == '-' {
			l.pos++
		} else {
			break
		}
	}
	return token{tokIDENT, l.src[start:l.pos], pos}
}

// lexDuration scans a Go-style duration literal like "5m", "1h30m".
// The full string is passed to time.ParseDuration by the parser.
func (l *lexer) lexDuration(pos int) token {
	start := l.pos
	// Consume all digits and ASCII letters (covers ns, us, ms, s, m, h, d, w, y)
	for l.pos < len(l.src) {
		ch := l.src[l.pos]
		if isDigit(ch) || isLetter(ch) {
			l.pos++
		} else {
			break
		}
	}
	return token{tokDURATION, l.src[start:l.pos], pos}
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// ────────────────────────────────────────────────────────────────────────────
// Parser
// ────────────────────────────────────────────────────────────────────────────

type parser struct {
	lex    *lexer
	peeked *token
}

// peek returns the next token without consuming it.
func (p *parser) peek() token {
	if p.peeked == nil {
		t := p.lex.next()
		p.peeked = &t
	}
	return *p.peeked
}

// consume returns and discards the next token.
func (p *parser) consume() token {
	if p.peeked != nil {
		t := *p.peeked
		p.peeked = nil
		return t
	}
	return p.lex.next()
}

// expect consumes the next token and returns an error if it is not of type tt.
func (p *parser) expect(tt tokenType) error {
	t := p.consume()
	if t.typ != tt {
		return &ParseError{
			Pos: t.pos,
			Msg: fmt.Sprintf("expected %s, got %q", tokenName(tt), t.val),
		}
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Public entry point
// ────────────────────────────────────────────────────────────────────────────

// Parse parses a LogQL query string and returns its AST.
//
// Returns *ParseError for syntactically invalid input.
// Returns *UnsupportedError for valid LogQL that this proxy cannot translate.
func Parse(query string) (Query, error) {
	p := &parser{lex: &lexer{src: query}}
	q, err := p.parseQuery()
	if err != nil {
		return nil, err
	}
	if tok := p.peek(); tok.typ != tokEOF {
		return nil, &ParseError{
			Pos: tok.pos,
			Msg: fmt.Sprintf("unexpected token %q after query", tok.val),
		}
	}
	return q, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Grammar: query → metric_query | log_query
// ────────────────────────────────────────────────────────────────────────────

func (p *parser) parseQuery() (Query, error) {
	tok := p.peek()
	if tok.typ == tokIDENT && isAggFunc(tok.val) {
		return p.parseAggregationQuery()
	}
	if tok.typ == tokIDENT && isMetricFunc(tok.val) {
		return p.parseMetricQuery()
	}
	if tok.typ == tokLBRACE {
		return p.parseLogQuery()
	}
	return nil, &ParseError{
		Pos: tok.pos,
		Msg: fmt.Sprintf("expected { or metric function (count_over_time, rate), got %q", tok.val),
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Grammar: metric_query → func_name "(" log_query "[" DURATION "]" ")"
// ────────────────────────────────────────────────────────────────────────────

func (p *parser) parseMetricQuery() (*MetricQuery, error) {
	funcTok := p.consume()

	var fn MetricFunction
	switch funcTok.val {
	case "count_over_time":
		fn = CountOverTime
	case "rate":
		fn = Rate
	default:
		return nil, &UnsupportedError{Pos: funcTok.pos, Construct: funcTok.val}
	}

	if err := p.expect(tokLPAREN); err != nil {
		return nil, err
	}

	inner, err := p.parseLogQuery()
	if err != nil {
		return nil, err
	}

	if err := p.expect(tokLBRACKET); err != nil {
		return nil, err
	}

	durTok := p.consume()
	if durTok.typ != tokDURATION {
		return nil, &ParseError{
			Pos: durTok.pos,
			Msg: fmt.Sprintf("expected duration (e.g. 5m), got %q", durTok.val),
		}
	}
	dur, err := time.ParseDuration(durTok.val)
	if err != nil {
		return nil, &ParseError{
			Pos: durTok.pos,
			Msg: fmt.Sprintf("invalid duration %q: %v", durTok.val, err),
		}
	}

	if err := p.expect(tokRBRACKET); err != nil {
		return nil, err
	}
	if err := p.expect(tokRPAREN); err != nil {
		return nil, err
	}

	return &MetricQuery{Function: fn, Inner: *inner, Range: dur}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Grammar: log_query → stream_selector pipeline?
// ────────────────────────────────────────────────────────────────────────────

func (p *parser) parseLogQuery() (*LogQuery, error) {
	sel, err := p.parseStreamSelector()
	if err != nil {
		return nil, err
	}
	pipeline, err := p.parsePipeline()
	if err != nil {
		return nil, err
	}
	return &LogQuery{Selector: sel, Pipeline: pipeline}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Grammar: stream_selector → "{" (label_matcher ("," label_matcher)*)? "}"
// ────────────────────────────────────────────────────────────────────────────

func (p *parser) parseStreamSelector() (StreamSelector, error) {
	if err := p.expect(tokLBRACE); err != nil {
		return StreamSelector{}, err
	}

	var matchers []LabelMatcher

	for {
		tok := p.peek()
		if tok.typ == tokRBRACE {
			p.consume()
			break
		}
		if tok.typ == tokEOF {
			return StreamSelector{}, &ParseError{
				Pos: tok.pos,
				Msg: "unexpected end of input inside stream selector: missing }",
			}
		}

		// Comma separator between matchers
		if len(matchers) > 0 {
			if err := p.expect(tokCOMMA); err != nil {
				return StreamSelector{}, err
			}
			// Allow trailing comma: {app="api",}
			if p.peek().typ == tokRBRACE {
				p.consume()
				break
			}
		}

		m, err := p.parseLabelMatcher()
		if err != nil {
			return StreamSelector{}, err
		}
		matchers = append(matchers, m)
	}

	return StreamSelector{Matchers: matchers}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Grammar: label_matcher → IDENT op STRING
//
//	op → "=" | "!=" | "=~" | "!~"
//
// ────────────────────────────────────────────────────────────────────────────
func (p *parser) parseLabelMatcher() (LabelMatcher, error) {
	nameTok := p.consume()
	if nameTok.typ != tokIDENT {
		return LabelMatcher{}, &ParseError{
			Pos: nameTok.pos,
			Msg: fmt.Sprintf("expected label name, got %q", nameTok.val),
		}
	}

	opTok := p.consume()
	var mt MatchType
	switch opTok.typ {
	case tokEQ:
		mt = Eq
	case tokNEQ:
		mt = Neq
	case tokRE:
		mt = Re
	case tokNRE:
		mt = Nre
	default:
		return LabelMatcher{}, &ParseError{
			Pos: opTok.pos,
			Msg: fmt.Sprintf("expected label operator (= != =~ !~), got %q", opTok.val),
		}
	}

	valTok := p.consume()
	if valTok.typ != tokSTRING {
		return LabelMatcher{}, &ParseError{
			Pos: valTok.pos,
			Msg: fmt.Sprintf("expected quoted string for label value, got %q", valTok.val),
		}
	}

	return LabelMatcher{Name: nameTok.val, Type: mt, Value: valTok.val}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Grammar: pipeline → pipeline_stage*
//
//	pipeline_stage → "|=" STRING       (contains)
//	               | "!=" STRING       (not-contains)
//	               | "|~" STRING       (regex)
//	               | "!~" STRING       (not-regex)
//	               | "|" "json"
//	               | "|" IDENT         (unsupported → UnsupportedError)
//
// ────────────────────────────────────────────────────────────────────────────
func (p *parser) parsePipeline() ([]PipelineStage, error) {
	var stages []PipelineStage

	for {
		tok := p.peek()
		switch tok.typ {
		case tokEOF, tokLBRACKET, tokRBRACKET, tokRPAREN:
			// End of pipeline: [ terminates when inside a metric range [5m];
			// ] and ) terminate when called from parseMetricQuery.
			return stages, nil

		case tokPIPE_EQ:
			p.consume()
			v, err := p.expectString(tok)
			if err != nil {
				return nil, err
			}
			stages = append(stages, &LineFilter{Op: Contains, Value: v})

		case tokPIPE_RE:
			p.consume()
			v, err := p.expectString(tok)
			if err != nil {
				return nil, err
			}
			stages = append(stages, &LineFilter{Op: Regex, Value: v})

		case tokNEQ:
			p.consume()
			v, err := p.expectString(tok)
			if err != nil {
				return nil, err
			}
			stages = append(stages, &LineFilter{Op: NotContains, Value: v})

		case tokNRE:
			p.consume()
			v, err := p.expectString(tok)
			if err != nil {
				return nil, err
			}
			stages = append(stages, &LineFilter{Op: NotRegex, Value: v})

		case tokPIPE:
			p.consume()
			identTok := p.consume()
			if identTok.typ != tokIDENT {
				return nil, &ParseError{
					Pos: identTok.pos,
					Msg: fmt.Sprintf("expected stage name after |, got %q", identTok.val),
				}
			}

			// If the next token is a comparison operator this is a label filter
			// stage (e.g. | labels.app.kubernetes.io/name!=""), not a parser
			// keyword like "json" or "logfmt".
			next := p.peek()
			switch next.typ {
			case tokEQ, tokNEQ, tokRE, tokNRE:
				p.consume() // consume the operator
				var mt MatchType
				switch next.typ {
				case tokEQ:
					mt = Eq
				case tokNEQ:
					mt = Neq
				case tokRE:
					mt = Re
				case tokNRE:
					mt = Nre
				}
				val, err := p.expectString(identTok)
				if err != nil {
					return nil, err
				}
				stages = append(stages, &LabelFilter{Name: identTok.val, Type: mt, Value: val})
			default:
				switch identTok.val {
				case "json":
					stages = append(stages, &JSONParser{})
				case "logfmt":
					stages = append(stages, &LogfmtParser{})
				case "drop", "keep":
					// "| drop field1, field2" and "| keep field1, field2" are
					// Loki pipeline stages that drop/retain specific fields.
					// VictoriaLogs does not have these fields (__error__,
					// __error_details__, …) so we parse and discard the stage.
					for {
						if p.peek().typ != tokIDENT {
							break
						}
						p.consume() // field name
						if p.peek().typ != tokCOMMA {
							break
						}
						p.consume() // comma
					}
				default:
					return nil, &UnsupportedError{
						Pos:       identTok.pos,
						Construct: "| " + identTok.val,
					}
				}
			}

		default:
			return nil, &ParseError{
				Pos: tok.pos,
				Msg: fmt.Sprintf("unexpected token %q in pipeline (expected |=, !=, |~, !~, | json/logfmt/drop/keep, or end of query)", tok.val),
			}
		}
	}
}

// expectString consumes a STRING token and returns its value, or an error.
// pipeOp is the preceding operator token, used for the error message.
func (p *parser) expectString(pipeOp token) (string, error) {
	t := p.consume()
	if t.typ != tokSTRING {
		return "", &ParseError{
			Pos: t.pos,
			Msg: fmt.Sprintf("expected quoted string after %q, got %q", pipeOp.val, t.val),
		}
	}
	return t.val, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Grammar: aggregation_query →
//
//	agg_func ["by" "(" label_list ")"] "(" metric_query ")" ["by" "(" label_list ")"]
//
// LogQL (and PromQL) allow the grouping clause in two equivalent positions:
//
//	sum by (label) (count_over_time({...}[5m]))   ← prefix form (common)
//	sum(count_over_time({...}[5m])) by (label)    ← postfix form (also valid)
//
// We accept both; if both are present the prefix wins (though Grafana never
// generates that combination).
//
//	agg_func  → "sum" | "count" | "avg" | "min" | "max"
//	label_list → IDENT ("," IDENT)*
//
// ────────────────────────────────────────────────────────────────────────────

func (p *parser) parseAggregationQuery() (*AggregationQuery, error) {
	funcTok := p.consume()

	var fn AggregationFunction
	switch funcTok.val {
	case "sum":
		fn = AggSum
	case "count":
		fn = AggCount
	case "avg":
		fn = AggAvg
	case "min":
		fn = AggMin
	case "max":
		fn = AggMax
	default:
		return nil, &ParseError{Pos: funcTok.pos,
			Msg: fmt.Sprintf("unknown aggregation function %q", funcTok.val)}
	}

	// Optional prefix "by (label, ...)" grouping clause.
	var by []string
	if tok := p.peek(); tok.typ == tokIDENT && tok.val == "by" {
		p.consume() // consume "by"
		var err error
		by, err = p.parseLabelList()
		if err != nil {
			return nil, err
		}
	}

	// The inner metric query is wrapped in an extra pair of parentheses:
	//   sum by (lbl) ( count_over_time({...}[5m]) )
	if err := p.expect(tokLPAREN); err != nil {
		return nil, err
	}
	inner, err := p.parseMetricQuery()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokRPAREN); err != nil {
		return nil, err
	}

	// Optional postfix "by (label, ...)" grouping clause:
	//   sum(count_over_time({...}[5m])) by (label)
	// Only used when no prefix clause was present.
	if by == nil {
		if tok := p.peek(); tok.typ == tokIDENT && tok.val == "by" {
			p.consume() // consume "by"
			var err error
			by, err = p.parseLabelList()
			if err != nil {
				return nil, err
			}
		}
	}

	return &AggregationQuery{Function: fn, By: by, Inner: *inner}, nil
}

// parseLabelList parses "(" IDENT ("," IDENT)* ")" and returns the names.
func (p *parser) parseLabelList() ([]string, error) {
	if err := p.expect(tokLPAREN); err != nil {
		return nil, err
	}
	var names []string
	for {
		if p.peek().typ == tokRPAREN {
			p.consume()
			break
		}
		if len(names) > 0 {
			if err := p.expect(tokCOMMA); err != nil {
				return nil, err
			}
			// Allow trailing comma.
			if p.peek().typ == tokRPAREN {
				p.consume()
				break
			}
		}
		identTok := p.consume()
		if identTok.typ != tokIDENT {
			return nil, &ParseError{
				Pos: identTok.pos,
				Msg: fmt.Sprintf("expected label name in grouping list, got %q", identTok.val),
			}
		}
		names = append(names, identTok.val)
	}
	return names, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func isAggFunc(name string) bool {
	switch name {
	case "sum", "count", "avg", "min", "max":
		return true
	}
	return false
}

func isMetricFunc(name string) bool {
	return name == "count_over_time" || name == "rate"
}

func tokenName(tt tokenType) string {
	switch tt {
	case tokLBRACE:
		return `"{"`
	case tokRBRACE:
		return `"}"`
	case tokLPAREN:
		return `"("`
	case tokRPAREN:
		return `")"`
	case tokLBRACKET:
		return `"["`
	case tokRBRACKET:
		return `"]"`
	case tokCOMMA:
		return `","`
	case tokIDENT:
		return "identifier"
	case tokSTRING:
		return "string"
	case tokDURATION:
		return "duration"
	case tokEOF:
		return "EOF"
	default:
		return fmt.Sprintf("token(%d)", int(tt))
	}
}
