h1_count = 0
h2_count = 0
h3_count = 0

h1_title = ''
h2_title = ''
h3_title = ''

md_file = None

summary_file = open('SUMMARY.md', 'w')
summary_file.write('# Summary\n\n')

f = open('flinktutorial.md', 'r')

def convert_title(i):
    if i < 10:
        return '0' + str(i)
    else:
        return str(i)

def retrieve_title(s):
    arr = s.split()[1:]
    return '-'.join(arr)

for line in f.readlines():
    if line.startswith('# '):
        h1_count += 1
        h2_count = 0
        h3_count = 0
        h1_title = retrieve_title(line)
        file_title = 'chapter' + convert_title(h1_count) + '-' + h1_title + '.md'

        try:
            md_file.close()
        except:
            pass

        md_file = open(file_title, 'w')

        summary_file.write('*' + ' [' + h1_title + '](' + file_title + ')\n')

    if line.startswith('## '):
        h2_count += 1
        h3_count = 0
        h2_title = retrieve_title(line)
        file_title = 'chapter' + convert_title(h1_count) + '-' + convert_title(h2_count) + '-' + h2_title + '.md'

        try:
            md_file.close()
        except:
            pass

        md_file = open(file_title, 'w')

        summary_file.write('  *' + ' [' + h2_title + '](' + file_title + ')\n')

    if line.startswith('### '):
        h3_count += 1
        h3_title = retrieve_title(line)
        file_title = 'chapter' + convert_title(h1_count) + '-' + convert_title(h2_count) + '-' + convert_title(h3_count) + '-' + h3_title + '.md'

        try:
            md_file.close()
        except:
            pass

        md_file = open(file_title, 'w')

        summary_file.write('    *' + ' [' + h3_title + '](' + file_title + ')\n')

    md_file.write(line)

f.close()
try:
    md_file.close()
except:
    pass
summary_file.close()
