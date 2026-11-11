package analyze

import "mvdan.cc/sh/v3/syntax"

func Operator(node syntax.Node) string {
	switch node := node.(type) {
	case *syntax.CmdSubst:
		if node.Backquotes {
			return "`"
		}
		return "$("
	case *syntax.BinaryCmd:
		return node.Op.String()
	case *syntax.Redirect:
		return node.Op.String()
	case *syntax.UnaryArithm:
		return node.Op.String()
	case *syntax.BinaryArithm:
		return node.Op.String()
	case *syntax.ParamExp:
		switch {
		case node.Exp != nil:
			return node.Exp.Op.String()
		case node.Slice != nil:
			return ":"
		case node.Repl != nil:
			if node.Repl.All {
				return "//"
			}
			return "/"
		case node.Names != 0:
			return "!" + node.Names.String()
		case node.Length:
			return "#"
		case node.Excl:
			return "!"
		}
	}
	return ""
}
