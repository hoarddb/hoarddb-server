package cli

import (
	"fmt"
	"strings"
)

// ringGroupTitles are the section headings of 'help', worded like v1.0's.
var ringGroupTitles = map[string]string{
	"Schema": "Schema Management", "Read": "Read Operations", "Write": "Write Operations", "Store": "Store Fields (each document's own; help types)",
	"Users":   "User Management",
	"Cluster": "Cluster Management", "Session": "Session Commands",
}

// ringQuickColumn is the width of the statement column of the Quick Start page.
const ringQuickColumn = 42

// Help returns the text of 'help': the Quick Start, the first five minutes in
// the order a session types them, each line a statement that runs as printed.
// It is generated from ringQuickStart, whose examples must be examples of the grammar table.
func Help() string {
	const title = "hoardDB Query Language (HQL) \u2014 Quick Start"
	var out strings.Builder
	out.WriteString(title + "\n" + strings.Repeat("=", len([]rune(title))) + "\n\n")
	for _, line := range ringQuickStart {
		if line.Heading != "" {
			fmt.Fprintf(&out, "%s:\n", line.Heading)
		}
		for _, row := range ringColumns(line.Example, ringQuickColumn, []string{line.Note}) {
			out.WriteString(row + "\n")
		}
	}
	out.WriteString("\nMore: 'help <word>' shows every form of one statement, as in 'help push';\n" +
		"'help all' lists every statement. Change or remove: help update, help delete.\n" +
		"Tab completes statements and names. 'exit' leaves.\n")
	return out.String()
}

// ringAllColumn is the width of the syntax column of 'help all'; a longer
// syntax is followed by two spaces and its description.
const ringAllColumn = 44

// ringScreenWidth is the widest line any help page prints.
const ringScreenWidth = 80

// HelpAll returns the text of 'help all': every statement ring mode runs with its
// syntax and one short description, no examples, then one block that names the
// statements it cannot run yet. Every row is generated from the grammar table.
func HelpAll() string {
	const title = "hoardDB Query Language (HQL) \u2014 Every Statement"
	var out strings.Builder
	out.WriteString(title + "\n" + strings.Repeat("=", len([]rune(title))) + "\n")
	out.WriteString("\n'help <word>' shows one statement with examples. Tab completes every word.\n")
	column := 0
	for _, form := range ringForms {
		if !form.Unavailable {
			column = max(column, min(ringWidth(form.Syntax()), ringAllColumn))
		}
	}
	for _, group := range ringGroups {
		var rows []string
		for _, form := range ringForms {
			if form.Group != group || form.Unavailable {
				continue
			}
			short := []string{form.Short}
			if 2+ringWidth(form.Syntax())+2+ringWidth(form.Short) > ringScreenWidth {
				// A syntax wider than the column leaves too little room: the description goes on the next row.
				short = []string{"", form.Short}
			}
			if form.Verb == "field" {
				short = ringWrapWords(strings.Join(ringVerbNames(), " "), ringScreenWidth-(2+column+2))
			}
			rows = append(rows, ringColumns(form.Syntax(), column, short)...)
		}
		if group == "Session" {
			rows = append(rows, ringColumns("exit", column, []string{"Leave; quit works too."})...)
		}
		if len(rows) > 0 {
			fmt.Fprintf(&out, "\n%s:\n%s", ringGroupTitles[group], strings.Join(rows, "\n")+"\n")
		}
	}
	var missing []string
	for _, form := range ringForms {
		if form.Unavailable {
			missing = append(missing, form.Verb)
		}
	}
	fmt.Fprintf(&out, "\nNot available yet:\n")
	for _, line := range ringWrapList(missing, ringScreenWidth-2) {
		fmt.Fprintf(&out, "  %s\n", line)
	}
	return out.String()
}

// ringColumns prints a syntax in the left column and the first description line beside it; a
// syntax wider than the column is followed by two spaces. More description lines align under the first.
func ringColumns(syntax string, column int, description []string) []string {
	var rows []string
	for i, line := range description {
		left := ""
		if i == 0 {
			left = syntax
		}
		gap := "  "
		if ringWidth(left) < column {
			gap = strings.Repeat(" ", column-ringWidth(left)+2)
		}
		rows = append(rows, strings.TrimRight("  "+left+gap+line, " "))
	}
	return rows
}

// ringWidth is the number of columns a plain line takes: its characters, not its bytes.
func ringWidth(text string) int { return len([]rune(text)) }

// ringWrapWords breaks text into lines of at most width characters at spaces.
func ringWrapWords(text string, width int) []string {
	var lines []string
	line := ""
	for _, word := range strings.Fields(text) {
		switch {
		case line == "":
			line = word
		case len([]rune(line))+1+len([]rune(word)) <= width:
			line += " " + word
		default:
			lines = append(lines, line)
			line = word
		}
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

// ringWrapList joins items with commas into lines of at most width characters; an item never splits.
func ringWrapList(items []string, width int) []string {
	var lines []string
	line := ""
	for i, item := range items {
		if i < len(items)-1 {
			item += ","
		}
		switch {
		case line == "":
			line = item
		case len([]rune(line))+1+len([]rune(item)) <= width:
			line += " " + item
		default:
			lines = append(lines, line)
			line = item
		}
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}
