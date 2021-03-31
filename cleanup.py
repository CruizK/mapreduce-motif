import sys


MOTIF = sys.argv[1]
INPUT_FILE = sys.argv[2]

final_output = []

if len(sys.argv) < 3:
  print("USAGE: cleanup.py MOTIF INPUT_FILE")

def getDiff(a, b):
  diff = 0
  for i in range(len(a)):
    if a[i] != b[i]:
      diff += 1
  return diff

final_diff = 0
with open(INPUT_FILE, 'r') as f:
  lines = f.readlines()
  line_count = 0

  for line in lines:
    final_pos = 0
    dis = 0
    match_motif = ""
    my_line = 0
    pos = 0
    min_diff = 1000000
    while pos+8 <= len(line):
      sub = line[pos:pos+8]
      diff = getDiff(MOTIF, sub)
      if diff < min_diff:
        min_diff = diff
        match_motif = sub
        final_pos = pos
        my_line = line_count
      pos += 1
    final_diff += min_diff
    final_output.append({
      "motif": MOTIF,
      "match_motif": match_motif,
      "seq_id": my_line,
      "dis": min_diff,
      "index": final_pos
    })
    line_count += 1


def print_num_pretty(num):
  if isinstance(num, str):
    return '{0}\t'.format(num)
  elif num > 9:
    return '{0}\t'.format(num)
  else:
    return ' {0}\t'.format(num)


with open('output.txt', 'w') as f:
  f.write('Motif\tMatchMotif\tSeqID\tDis\tIndex\ttotalDis\n')
  for output in final_output:
    for key in output:
      f.write(print_num_pretty(output[key]))
    f.write('{0}\n'.format(final_diff))



