import os
import os
import pandas as pd
from dateutil.parser import parse
from datetime import datetime

"""
Review data preprocessing
"""

class Dir_List:

    def __init__(self,path_,exten):
        self.path_exten = [path_+"\\"+f for f in os.listdir(path_) if f.endswith(".{}".format(exten))]
        self.name_exten = [f for f in os.listdir(path_) if f.endswith(".{}".format(exten))]
        #return self.path_exten,self.name_exten

    def data_list(self,path_,exten):
        self.path_exten = [path_+"\\"+f for f in os.listdir(path_) if f.endswith(".{}".format(exten))]
        self.name_exten = [f for f in os.listdir(path_) if f.endswith(".{}".format(exten))]
        return self.path_exten,self.name_exten


# kwargs 는 유지시키고,
# args 에는 하나를 추가한다.


class Data_Load(Dir_List):



    def __init__(self,remove_col,*args):
        super(Data_Load,self).__init__(*args)
        self.df = pd.DataFrame()
        for pa_,na_ in zip(self.path_exten,self.name_exten):
            df_ = pd.read_excel(pa_).drop(columns="Unnamed: 0")
            df_["Product Name"] = na_
            self.df = pd.concat([self.df,df_]).reset_index(drop=True)
        if remove_col !=None:
            self.df = self.df[self.df[remove_col] != "None"].reset_index(drop=True)
        else:
            pass


    def parsing_date(self,target_):
        parsing = [parse(d) for d in self.df[target_]]
        self.df["parse_date"] = parsing

        self.df["year"] = [d.strftime("%Y") for d in self.df["parse_date"]]
        self.df["month"] = [d.strftime("%Y-%m") for d in self.df["parse_date"]]
        self.df["day"] = [d.strftime("%Y-%m-%d") for d in self.df["parse_date"]]
        return self.df


import pandas as pd
import kss
from tqdm.notebook import tqdm
from konlpy.tag import Kkma,Okt
okt = Okt()
kkma = Kkma()

class Pos_Process:



    def __init__(self,df,target_):
        self.df = df
        sent_li = []
        tq = tqdm(range(len(self.df)),desc="Split Sentences")
        for i in tq:
            try:
                sen_ = kss.split_sentences(df[target_][i])
                sent_li.append(sen_)
            except:
                sent_li.append([])
        self.df["sentences"] = sent_li

    def pos_tagging(self,target_tag,Version):
        tag_li = []
        wo_li = []
        wo_li2 = []
        tag2_li = []
        tq = tqdm(range(len(self.df)),desc="Pos Tagging")
        for i in tq:
            sen = self.df["sentences"][i]
            tags_ = []
            wos_ = []
            wos_2 = []
            tags2_ = []
            for s in sen:
                if Version=="okt":
                    tag_ = okt.pos(s,stem=True)
                elif Version == "kkma":
                    tag_ = kkma.pos(s)
                word_ = [w[0] for w in tag_]
                if target_tag != []:
                    word_2 = [w[0] for w in  tag_ if w[1] in target_tag]
                    tag_2 = [w for w in  tag_ if w[1] in target_tag]
                else:
                    word_2 =[w[0] for w in  tag_ ]
                    tag_2 = [w for w in  tag_ ]
                tags_.append(tag_)
                wos_.append(word_)
                wos_2.append(word_2)
                tags2_.append(tag_2)
            tag_li.append(tags_)
            wo_li.append(wos_)
            wo_li2.append(wos_2)
            tag2_li.append(tags2_)
        self.df["sen_tag"] = tag_li
        self.df["sen_word"] = wo_li
        self.df["sen_word2"] = wo_li2
        self.df["sen_tag2"] = tag2_li
        return self.df

"""
dictionary data preprocessing
"""
import pandas as pd
from IPython.display import display
import ast

from konlpy.tag import Kkma,Okt
okt,kkma = Okt(),Kkma()



class Dic_Loading:


    def __init__(self,path,sheet=None):
        self.df = pd.read_excel(path,sheet_name=sheet).fillna("")

        ### change colnames
        display(self.df.loc[0:2,:])
        col_ = list(self.df.columns)
        print(col_)
        input_ = list(input("Inset New columns's name in order:").split(","))
        input_ = "key,attr,attr_sim,attr_combi,count,pos,neg".split(",") if input_==[""] else input_
        col_dic = {}
        for i in range(len(input_)):
            col_dic[col_[i]] = input_[i]
        self.df = self.df.rename(columns=col_dic)

        ### to list
        for col in self.df.columns:
            try:
                self.df[col] = self.df[col].map(lambda x: ast.literal_eval(x))
            except (ValueError,SyntaxError):
                try:
                    self.df[col] = self.df[col].map(lambda x: x.split("#")[1:] if "#" in x else x.split(",") if ","in x else [x])
                except (TypeError,SyntaxError,AttributeError):
                    self.df[col] = self.df[col].map(lambda x : [x])


    def load(self,path,sheet=None):
        self.df = pd.read_excel(path,sheet_name=sheet)
        display(self.df.head())
        col_ = list(self.df.columns)
        print(col_)
        input_ = list(input("Inset New columns's name in order:").split(","))
        col_dic = {}
        for i in range(len(input_)):
            col_dic[col_[i]] = input_[i]
        self.df = self.df.rename(columns=col_dic)
        return self.df




    def pos_tag(self,Version,Tag_dic,Tag_dic_n):


        def pos_tag_(List_se,Version):
            # list_se = ["바람도 쎄고","바람 세기 충분"]

            if Version=="kkma":
                pos_tag = []
                for f in List_se:
                    try:
                        pos_tag.append(kkma.pos(f))
                    except:
                        pos_tag.append([])
            elif Version =="okt":
                pos_tag = [ okt.pos(f,stem=True) for f in List_se]
            return pos_tag

        def tag_extract_(List_ta,Tag_dic,Stack_ver=None):
            # list_ta = [[('바람', 'Noun'), ], [('드라이기', 'Noun'), ('바람', 'Noun'), ('세기', 'Noun')]
            dic_tag = {}
            for key_,values_ in Tag_dic.items():
                for value_ in values_:
                    dic_tag[value_] = key_

            out_li = []
            for lt in List_ta:
                tag_ex = [i[0] for i in lt if i[1] in list(dic_tag.keys())]
                if Stack_ver==None:
                    out_li.append(tag_ex)
                elif Stack_ver=="extend":
                    out_li.extend(tag_ex)
            return out_li

        def delete_duple_(list_):
            tass = []
            for i in list_:
                if i in tass:
                    pass
                else:
                    tass.append(i)
            return tass

        def removal_list(Tester_,removal):
            for re in removal:
                try:
                    Tester_.remove(re)
                except:
                    pass
            return Tester_

        def removal_funda(fun_,remove_):
            for fu_ in fun_:
                if fu_ in remove_:
                    fun_.remove(fu_)
            return fun_

        def remove_list_pos_neg(pos_,remove_):
            remo_2 =[]
            for remo_ in remove_:
                remo_2.extend(remo_)
            pos_2 = []
            for remo_ in remo_2:
                for po_ in pos_:
                    if remo_ in po_:
                        po_.remove(remo_)
                        pos_2.append(po_)
            pos_2 = removal_list(delete_duple_(pos_2),[[]])
            return pos_2



        #### MAIN ###############
        tq = tqdm(range(len(self.df)),desc="Pos Tagging")

        self.dic = {}
        for i in tq:
            # key
            sk = tag_extract_(pos_tag_(self.df["key"][i],Version),Tag_dic)
            sk2 = [["".join(ke)] for ke in sk]
            sk3 = []
            for sk_ in sk:
                for sk__ in sk_:
                    sk3.append([sk__])
            key = sk + sk2 + sk3

            key = delete_duple_(key)
            removal2 = [['기능'],['감'], ['/'], ['및'],[]]
            key = removal_list(key,removal2)

            # fundamental = attr + attr_sim + attr_combi
            sa = tag_extract_(pos_tag_(self.df["attr"][i],Version),Tag_dic)
            sas =tag_extract_(pos_tag_(self.df["attr_sim"][i],Version),Tag_dic)
            #sac = tag_extract_(pos_tag_(self.df["attr_combi"][i],Version),Tag_dic)

            funda = sa + sas# + sac
            funda = delete_duple_(funda)


            # positive & negative
            positive = tag_extract_(pos_tag_(self.df["pos"][i],Version),Tag_dic)
            negative = tag_extract_(pos_tag_(self.df["neg"][i],Version),Tag_dic)

            funda = removal_funda(funda,positive)
            funda = removal_funda(funda,negative)


            # unioun intersetion
            spn = set(tag_extract_(pos_tag_(self.df["pos"][i],Version),Tag_dic_n,"extend"))
            snn = set(tag_extract_(pos_tag_(self.df["neg"][i],Version),Tag_dic_n,"extend"))

            noun_inter = [[ni] for ni in list(spn.intersection(snn))]
            noun_union = [[ni] for ni in list(spn.union(snn))]

            # remove stopwords
            removal = [['것'],['이'], ['왜'],['더'],[]]
            removal_pn = removal + key + funda


            key = removal_list(key,removal)
            funda = removal_list(funda,removal)
            positive = remove_list_pos_neg(positive,removal_pn)
            negative = remove_list_pos_neg(negative,removal_pn)



            noun_inter = removal_list(noun_inter,removal)
            noun_union = removal_list(noun_union,removal)





            dic_loop = {"key":key,"fundamental":funda,"positive":positive,"negative":negative,
                        "noun_inter":noun_inter,"noun_union":noun_union}
            self.dic[self.df["attr"][i][0]] = dic_loop
        return self.dic


"""
Evaluation model
"""


import pandas as pd
from tqdm.notebook import tqdm

class Model1:

    def __init__(self,df,target_col,target_col2):
        date_li,id_li,product_li,sen_li,score_li,liked_li,sentag_li,review_num,sen_num,cha_li = [],[],[],[],[],[],[],[],[],[]
        tq_ = tqdm(range(len(df)),desc="Preprocessing")
        sen_n = 1
        for i in tq_:
            for sen in df.loc[i,target_col]:
                review_num.append(i+1)
                date_li.append(df.loc[i,"date"])
                id_li.append(df.loc[i,"id"])
                product_li.append(df.loc[i,"Product Name"])
                score_li.append(df.loc[i,"score"])
                liked_li.append(df.loc[i,"liked"])
                cha_li.append(df.loc[i,"channel"])
                sen_li.append(sen)
                sen_num.append(sen_n)
                sen_n += 1
            sen_n = 1
            for sentag in df.loc[i,target_col2]:
                sentag_li.append(sentag)
        da = [date_li,id_li,product_li,score_li,liked_li,sen_li,sentag_li,review_num,sen_num,cha_li]
        self.df = pd.DataFrame(da).T.rename(columns={0:"date",1:"id",2:"Product Name",3:"score",4:"liked",5:"sentence",6:"sentag",7:"review_num",8:"sen_num",9:"channel"})

    def Model1_(self,dic,range_=None):

        def match_result_(Dic_lists,Ta_lists):
            def match_f_(dic_list,Ta_list):
                if all(elem in Ta_list for elem in dic_list):
                    return 1,dic_list
                else:
                    return 0,[""]

            score = list(map(lambda x:match_f_(x,Ta_lists)[0] ,Dic_lists))
            matching = list(map(lambda x:match_f_(x,Ta_lists)[1] ,Dic_lists))
            match_li = []
            [match_li.extend(em) for em in matching]
            return sum(score)," ".join(match_li)

        def model1_match_(Dic,Sen):
            out_li = []
            for key,value in Dic.items():
                list_ = ["fundamental","noun_inter","key","positive","negative","noun_union"]#list(value.keys())
                score_ = [match_result_(value[k],Sen)[0] for k in list_]
                string_ = [match_result_(value[k],Sen)[1] for k in list_]
                i = 0
                if score_[i] != 0:
                    out_li.append((key,i,score_))
                else:
                    i += 1
                    while score_[i] != 0:
                        out_li.append((key,i,score_))
                        i += 1
                        if i ==len(list_):
                            break
            return out_li

        ### Main ####
        model1_li = []
        if range_ != None:
            df2 = self.df.loc[range_[0]:range_[1]]
            tq = tqdm(df2["sentence"],desc="Model Mathcing")
            for sen in tq:
                model1_li.append([(i[0],i[1],i[2]) for i in model1_match_(dic,sen)])
            df2["model1_match"] = model1_li
            return df2
        else:
            tq = tqdm(self.df["sentence"],desc="Model Mathcing")
            for sen in tq:
                model1_li.append([(i[0],i[1],i[2]) for i in model1_match_(dic,sen)])
            self.df["model1_match"] = model1_li
            return self.df
