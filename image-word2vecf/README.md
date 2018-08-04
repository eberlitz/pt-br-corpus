docker build -t word2vecf:latest .
docker run -it word2vecf:latest /bin/bash

docker run -it -v /home/berlitz/data:/usr/src/app/data word2vecf:latest 


# Generate word vocabulary file
docker run -it -v /home/berlitz/data:/usr/src/app/data word2vecf:latest python ./scripts/contexts.py ./data/ptwiki.db -o ./data/contexts/ -wv 50 -b 1000 

# Generate word,context and context vocabulary files
docker run -it -v /home/berlitz/data:/usr/src/app/data word2vecf:latest python ./scripts/contexts.py ./data/ptwiki.db -b 1000 -o ./data/contexts/ -mc 100

# train word2vecf
docker run -it -v /home/berlitz/data:/usr/src/app/data word2vecf:latest ./word2vecf/word2vecf -train ./data/contexts/dep.contexts -wvocab ./data/contexts/wv -cvocab ./data/contexts/cv -output ./data/dim200vecs -size 200 -negative 15 -threads 10

# convert to numpy vectors
docker run -it -v /home/berlitz/data:/usr/src/app/data word2vecf:latest python ./scripts/vecs2nps.py ./data/dim200vecs ./data/vecs




curl https://eberlitz.blob.core.windows.net/ptwiki2vec/images/image-word2vecf.tar -o ./image-word2vecf.tar