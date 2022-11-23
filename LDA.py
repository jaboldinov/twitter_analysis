#!/usr/bin/python3.8

import gensim
import gensim.corpora as corpora
import gensim.utils as utils
import credentials


class LDA:
    def __init__(self):
        self.model_path = credentials.ldapath
        self.model =  gensim.models.LdaModel.load(self.model_path)
        self.dictionary = corpora.Dictionary.load(credentials.dictpath)


    def get_topic(self, text, topic):
        text = utils.tokenize(text)
        ques_vec = self.dictionary.doc2bow(text)
        topic_vec = self.model[ques_vec]
        output = topic_vec[topic][1]

        return output