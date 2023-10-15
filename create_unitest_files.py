from random import shuffle
import os

word_count = {'Hello': 30,
              'world': 351,
              'World': 210,
              'This': 98,
              'is': 80,
              'hello': 7,
              'nonsense': 142}

input_path = 'test1'
n_files = 10

# create or clean input_path:
if os.path.exists(input_path):
    for f in os.listdir(input_path):
        if f.endswith('.txt'):
            os.remove(os.path.join(input_path, f))
else:
    os.mkdir(input_path)

# create word list
all_words = []
for k, v in word_count.items():
    all_words.extend([k]*v)
shuffle(all_words)

# write into txt files
k, m = divmod(len(all_words), n_files)
for i in range(n_files):
    output_filename = os.path.join(input_path, f'text_{i}.txt')
    words = ' '.join(all_words[i*max(1, k):(i+1)*max(1, k)])
    with open(output_filename, 'w') as f:
        f.write(words + '\n')
if m > 0 and k > 0:
    output_filename = os.path.join(input_path, f'text_0.txt')
    words = ' '.join(all_words[-m:])
    with open(output_filename, 'a') as f:
        f.write(words + '\n')
