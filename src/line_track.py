#!/usr/bin/env python

import re

def main():
	lines = []
	with open('case2', 'r') as inF:
		for line in inF.readlines():
			lines.append(line.rstrip())
	nline = len(lines)

	prog1 = re.compile(r'^goroutine \d+ \[')
	prog2 = re.compile('^\t(.+):(\d+)')

	cnt = {}

	i = 0
	while i < nline:
		if not prog1.search(lines[i]):
			i += 1
		else:
			m = prog2.search(lines[i + 2])
			assert(m != None)
			cf = m.group(1)
			cl = int(m.group(2))
			key = '%s:%d' % (cf, cl)
			cnt[key] = cnt.get(key, 0) + 1
			i += 3

	for k, v in cnt.items():
		print 'Cnt: %4d, Loc: %s' % (v, k)

main()
