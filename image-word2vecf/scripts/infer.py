import heapq
import numpy as np


def ugly_normalize(vecs):
    normalizers = np.sqrt((vecs * vecs).sum(axis=1))
    normalizers[normalizers == 0] = 1
    return (vecs.T / normalizers).T


class Embeddings:
    def __init__(self, vecsfile, vocabfile=None, normalize=True):
        if vocabfile is None:
            vocabfile = vecsfile.replace("npy", "vocab")
        self._vecs = np.load(vecsfile)
        self._vocab = open(vocabfile, 'r').read().split()
        if normalize:
            self._vecs = ugly_normalize(self._vecs)
        self._w2v = {w: i for i, w in enumerate(self._vocab)}

    @classmethod
    def load(cls, vecsfile, vocabfile=None):
        return Embeddings(vecsfile, vocabfile)

    def word2vec(self, w):
        return self._vecs[self._w2v[w]]

    def similar_to_vec(self, v, N=10):
        sims = self._vecs.dot(v)
        sims = heapq.nlargest(N, zip(sims, self._vocab, self._vecs))
        return sims

    def most_similar(self, word, N=10):
        w = self._vocab.index(word)
        sims = self._vecs.dot(self._vecs[w])
        sims = heapq.nlargest(N, zip(sims, self._vocab))
        return sims

    def analogy(self, pos1, neg1, pos2, N=10, mult=True):
        wvecs, vocab = self._vecs, self._vocab
        p1 = vocab.index(pos1)
        p2 = vocab.index(pos2)
        n1 = vocab.index(neg1)
        if mult:
            p1, p2, n1 = [(1+wvecs.dot(wvecs[i]))/2 for i in (p1, p2, n1)]
            if N == 1:
                return max(((v, w) for v, w in zip((p1 * p2 / n1), vocab) if w not in [pos1, pos2, neg1]))
            return heapq.nlargest(N, ((v, w) for v, w in zip((p1 * p2 / n1), vocab) if w not in [pos1, pos2, neg1]))
        else:
            p1, p2, n1 = [(wvecs.dot(wvecs[i])) for i in (p1, p2, n1)]
            if N == 1:
                return max(((v, w) for v, w in zip((p1 + p2 - n1), vocab) if w not in [pos1, pos2, neg1]))
            return heapq.nlargest(N, ((v, w) for v, w in zip((p1 + p2 - n1), vocab) if w not in [pos1, pos2, neg1]))

# if __name__ == '__main__':
#    import sys

#    e = Embeddings.load(sys.argv[1])

#    print e.most_similar('azkaban')
#    print e.analogy('king','man','woman')
