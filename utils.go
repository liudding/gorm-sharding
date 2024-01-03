package sharding

import (
	"regexp"
	"strconv"
	"strings"
)

var fromTableRegexp = regexp.MustCompile("(?i)(?:FROM|UPDATE|MERGE INTO|INSERT [a-z ]*INTO) ['`\"]?([a-zA-Z0-9_]+)([ '`\",)]|$)")
var whereRegexp = regexp.MustCompile("(?i)(?:WHERE|AND) ['`\"]?([a-zA-Z0-9_]+)['`\"]? = (['`\"]?[a-zA-Z0-9_?\\-]+['`\"]?)")

func getTableFromRawSQL(sql string) string {
	if matches := fromTableRegexp.FindAllStringSubmatch(sql, -1); len(matches) > 0 {
		return matches[0][1]
	}

	return ""
}

func GetWhereValueFromRawSQL(sql string) [][]string {
	if matches := whereRegexp.FindAllStringSubmatch(sql, -1); len(matches) > 0 {
		m := make([][]string, len(matches))
		for i, match := range matches {
			m[i] = []string{match[1], match[2]}
		}

		return m
	}

	return nil
}

func getSQLWhereValueOfVar(col, sql string, args ...any) any {
	vars := GetWhereValueFromRawSQL(sql)

	i := -1
	for _, kv := range vars {
		if kv[0] == col {
			if kv[1] == "?" {
				i++
				return args[i]
			}

			if strings.HasPrefix(kv[1], "'") {
				return strings.Trim(kv[1], "'")
			} else {
				parseInt, err := strconv.ParseInt(kv[1], 10, 64)
				if err != nil {
					return nil
				}
				return parseInt
			}
		}
	}
	return nil
}

func getInsertValuesFromRawSQL(sql string) {
	sql = strings.ToLower(sql)
	strings.Split(sql, "values")
}
