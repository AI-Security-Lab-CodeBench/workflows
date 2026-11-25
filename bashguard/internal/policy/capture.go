package policy

import (
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
)

const publicCLITokens = `
	alias ar asa at awk basename bc bg c99 cal cat cd cflow chgrp chmod chown cksum cmp comm command
	compress continue cp crontab csplit ctags cut date dd df diff dirname du echo ed env ex exec exit
	expand export expr false fc fg file find fold fuser gencat getconf getopts grep hash head iconv id
	ipcrm ipcs jobs join kill lex link ln locale localedef logger logname lp ls m4 mailx make man mesg
	mkdir mkfifo more mv newgrp nice nl nohup od paste patch pathchk pax pr printf ps pwd read readonly
	renice rm rmdir sed sh sleep sort split strings strip stty tabs tail talk tee test time touch tput
	tr trap true tsort tty type ulimit umask unalias uname uncompress unexpand uniq unlink unset
	uudecode uuencode vi wait wc what who write xargs yacc zcat break
	base32 base64 basenc b2sum chcon chroot dir dircolors factor fmt groups install md5sum
	mktemp nproc numfmt pinky printenv ptx readlink realpath seq sha1sum sha224sum sha256sum sha384sum
	sha512sum shred shuf stat stdbuf sum tac timeout truncate users vdir whoami yes
	blkid column dmesg fallocate flock free getent hexdump hostname ip journalctl lsblk lscpu lsns
	mount namei nsenter partx pgrep pidof pkill prlimit pstree readprofile rev runuser setarch
	setpriv setsid ss su swapon switch_root systemctl taskset unshare uptime vmstat wall whereis
	-F -v -f -n -b --number --number-nonblank -u --utc --universal -q --quiet --silent --verbose
	-l -a -d -h -la -al --all -L -P --logical --physical --dereference --file-system
	-c -m -w --bytes --lines --decode -i --ignore-garbage
`

var captureTokens = func() map[string]struct{} {
	tokens := make(map[string]struct{})
	for _, token := range strings.Fields(publicCLITokens) {
		tokens[token] = struct{}{}
	}
	for name := range syntaxTypes() {
		tokens[strings.TrimPrefix(name, "syntax.")] = struct{}{}
	}
	for _, token := range []string{
		"", "-", "--", "<", ">", ">>", "<<", "<<<", "<>", ">|", "<&", ">&", "|", "|&", "&>", "&>>",
		"&&", "||", "$(", "`", "unknown", "production", "nonproduction", "granted", "not_granted",
	} {
		tokens[token] = struct{}{}
	}
	return tokens
}()

func validateCaptureExpression(rule Rule, checked *cel.Ast) error {
	var issue error
	ast.PreOrderVisit(checked.NativeRep().Expr(), ast.NewExprVisitor(func(expr ast.Expr) {
		if issue != nil {
			return
		}
		switch expr.Kind() {
		case ast.IdentKind:
			if expr.AsIdent() == "source" {
				issue = captureError(rule, "learned predicates cannot read raw Bash source")
			}
		case ast.SelectKind:
			if slices.Contains([]string{"Raw", "Value", "Text", "raw", "value", "text", "start", "end"}, expr.AsSelect().FieldName()) {
				issue = captureError(rule, "learned predicates cannot read raw or positional AST values")
			}
		case ast.CallKind:
			if slices.Contains([]string{"start", "end", "offset"}, expr.AsCall().FunctionName()) {
				issue = captureError(rule, "learned predicates cannot read raw or positional AST values")
			}
		case ast.LiteralKind:
			switch value := expr.AsLiteral().(type) {
			case types.String:
				if _, ok := captureTokens[string(value)]; !ok {
					issue = captureError(rule, "use documented public tokens and structural AST symbols, not raw literals")
				}
			case types.Bytes:
				issue = captureError(rule, "learned predicates cannot use byte literals")
			}
		default:
		}
	}))
	return issue
}

func captureError(rule Rule, message string) error {
	return &RuleError{RuleID: rule.ID, Field: "when", Code: "raw_literal_overfit", Detail: message}
}

func codecArgumentRoles(argv []string) []string {
	roles := make([]string, 0, len(argv)-1)
	options, operand := true, false
	for i := 1; i < len(argv); i++ {
		value := argv[i]
		switch {
		case operand:
			return nil
		case options && value == "-w":
			i++
			if i == len(argv) || !codecWidth(argv[i]) {
				return nil
			}
			roles = append(roles, "-w", "@decimal")
		case options && slices.Contains([]string{"-d", "--decode", "-i", "--ignore-garbage"}, value):
			roles = append(roles, value)
		case options && value == "--":
			options = false
			roles = append(roles, value)
		case value == "-":
			operand = true
			roles = append(roles, value)
		case value == "" || (options && strings.HasPrefix(value, "-")) || strings.ContainsAny(value, "*?["):
			return nil
		default:
			operand = true
			roles = append(roles, "")
		}
	}
	return roles
}

func codecWidth(value string) bool {
	return len(value) >= 1 && len(value) <= 4 && strings.IndexFunc(value, func(r rune) bool {
		return r < '0' || r > '9'
	}) == -1
}
