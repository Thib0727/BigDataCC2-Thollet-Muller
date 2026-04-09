https://github.com/Thib0727/BigDataCC2-Thollet-Muller


## PREPARATION

#### Décompresser le fichier
```
unzip ml-25m.zip
```
#### verification
```
ls -l
```

### Envoi sur hdfs 
```
hdfs dfs -put ml-25m/tags.csv
```

#### On envoie aussi le fichier de petite taille

### Envoi vers la VM 

#### Dans un nouveau cmd on execute
```
scp -P 2222 "C:\Users\ythollet\Downloads\echantillon-tags.csv" maria_dev@localhost:~
```
### Envoi sur hdfs 
```
hdfs dfs -put echantillon-tags.csv
```

## 1- Combien de tags chaque film possède-t-il ?
```
nano tags_per_movie.py
```
On copie-colle le code python

```
-*- coding: utf-8 -*-
from mrjob.job import MRJob

class TagsPerMovie(MRJob):

    def mapper(self, _, line):
        # Ignorer la ligne d'en-tête
        if line.startswith("userId"):
            return
            
        try:
            # On découpe par virgule
            row = line.split(',')
            
            # L'ID du film est toujours la deuxième colonne (index 1)
            movie_id = row[1]
            
            yield movie_id, 1
        except Exception:
            pass

    def reducer(self, movie_id, counts):
        # On fait la somme de tous les 1 reçus pour un même film
        yield movie_id, sum(counts)

if __name__ == '__main__':
    TagsPerMovie.run()
```
On sauvergarde et quitte nano

#### On lance le script python (dans un premier temps sur l'échantillon puis sur le fichier complet)et on stocke le resultat dans un fichier txt
python tags_per_movie.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar tags.csv > res.txt



#### On visualise le contenu de resultat
```
[maria_dev@sandbox-hdp ~]$ head -20 res.txt
"1"     697
"10"    137
"100"   18
"1000"  10
"100001"        1
"100003"        3
"100008"        9
"100017"        9
"100032"        2
"100034"        19
"100036"        1
"100038"        4
"100042"        2
"100044"        12
"100046"        3
"100048"        1
"100052"        4
"100054"        6
"100060"        10
"100062"        2
```

### Analyse des logs
##### [Logs complets](https://github.com/Thib0727/BigDataCC2-Thollet-Muller/blob/main/log_q1.txt)

Sur un total de 1 093 361 lignes lues, le Mapper a généré exactement 1 093 360 couples clé-valeur (on retire la ligne des entêtes). La phase de réduction a ensuite agrégé ce million de lignes en 45 251 groupes uniques, ce qui correspond au nombre précis de films ayant reçu au moins une évaluation.

Le framework a segmenté le fichier d'entrée en deux blocs distincts (Launched map tasks = 2), permettant un traitement parallèle qui divise le temps d'exécution par 2. Avec un temps de traitement cumulé d'environ 30 secondes pour plus d'un million d'enregistrements, les performances sont excellentes pour un environnement Sandbox.


## 2- Combien de tags chaque utilisateur a-t-il ajoutés ?
```
nano tags_per_user.py
```

#### On copie-colle le code python
```
-*- coding: utf-8 -*-
from mrjob.job import MRJob

class TagsPerUser(MRJob):

    def mapper(self, _, line):
        # Ignorer la ligne d'en-tête
        if line.startswith("userId"):
            return
            
        try:
            # On découpe par virgule
            row = line.split(',')
            
            # L'ID utilisateur est toujours la toute première colonne (index 0)
            user_id = row[0]
            
            yield user_id, 1
        except Exception:
            pass

    def reducer(self, user_id, counts):
        yield user_id, sum(counts)

if __name__ == '__main__':
    TagsPerUser.run()
```


#### On sauvergarde et quitte nano

#### On lance le script python et on stocke le resultat dans un fichier txt
```
[maria_dev@sandbox-hdp ~]$ python tags_per_user.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-cl
ient/hadoop-streaming.jar ml-25m/tags.csv > res.txt
```

#### On visualise le contenu de resultat
```
head -20 res.txt

"100001"        9
"100016"        50
"100028"        4
"100029"        1
"100033"        1
"100046"        133
"100051"        19
"100058"        5
"100065"        2
"100068"        19
"100076"        4
"100085"        3
"100087"        8
"100088"        13
"100091"        29
"100101"        3
"100125"        3
"100130"        2
"100140"        5
"100141"        26
```
### Analyse des logs
#### [Logs complets](https://github.com/Thib0727/BigDataCC2-Thollet-Muller/blob/main/log_q2.txt)

Le Mapper a correctement filtré le fichier des 1 093 360 enregistrements utiles (sans la ligne d'entête). La phase de réduction a permis d'identifier 14 592 utilisateurs uniques. Les compteurs de sortie correspondent aux groupes d'entrée ce qui confirme que le traitement s'est bien déroulé.

Le temps de calcul total, réparti est de 17,8s pour le mapping et 8,9s pour produire enfin le fichier final res.txt.


## 3- Combien de blocs le fichier occupe-t-il dans HDFS dans chacune des configurations ?
```
hdfs fsck ml-25m/tags.csv -files -blocks
```

```
Total size:    38810332 B 
Total blocks (validated):      1 (avg. block size 38810332 B)
```
le fichier a donc une taille totale de 38 mo, meme si on reduit la taille a 64mo ca donnera le meme résultat, essayons quand meme:
```
hdfs dfs -D dfs.blocksize=67108864 -put ml-25m/tags.csv /tags_64mo.csv
```
si on regarde ce qu'on obtient en sortie avec hdfs fsck /tags_64mo.csv -files -blocks, cela confrime notre hypothèse:

```
Total size:    38810332 B
Total blocks (validated):      1 (avg. block size 38810332 B)
```


## 4- Combien de fois chaque tag a-t-il été utilisé pour taguer un film ?

#### On créé un fichier python
```
nano tag_use_film.py
```

On copie-colle le code python

```
-*- coding: utf-8 -*-
from mrjob.job import MRJob

class FrequenceTags(MRJob):

    def mapper(self, _, line):
        if line.startswith("userId"):
            return
            
        try:
            row = line.split(',')
            # Dans le fichier, l'index 2 correspond au texte du Tag
            tag = row[2]
            # On met le tag tout en minuscules pour éviter que "Drôle" et "drôle" soient comptés séparément
            tag = tag.strip().lower() 
            
            yield tag, 1
        except Exception:
            pass

    def reducer(self, tag, counts):
        # On fait la somme de toutes les fois où ce tag précis a été utilisé
        yield tag, sum(counts)

if __name__ == '__main__':
    FrequenceTags.run()
```
#### On lance le script python 

```
python tag_use_film.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar ml-25m/tags.csv > res.txt
```


#### On visualise le contenu du resultat

```
head -20 res.txt

"!950's superman tv show"       1
"#1 prediction" 3
"#adventure"    1
"#antichrist"   1
"#boring #lukeiamyourfather"    1
"#boring"       1
"#danish"       2
"#documentary"  1
"#entertaining" 1
"#exorcism"     1
"#fantasy"      2
"#hanks #muchstories"   1
"#jesus"        1
"#lifelessons"  1
"#lukeiamyourfather"    1
"#metoo"        1
"#mindfulness"  1
"#notscary"     1
"#rap"  1
"#science"      1
```


## 5- Pour chaque film, combien de tags le même utilisateur a-t-il introduits ?

#### On créé un fichier python
```
nano tags_duo.py
```

On copie-colle le code python

```
-*- coding: utf-8 -*-
from mrjob.job import MRJob

class TagsDuo(MRJob):

    def mapper(self, _, line):
        # Ignorer l'en-tête
        if line.startswith("userId"):
            return
            
        try:
            row = line.split(',')
            user_id = row[0]
            movie_id = row[1]
            
            # On émet un tuple (Film, Utilisateur) comme clé
            yield (movie_id, user_id), 1
        except Exception:
            pass

    def reducer(self, duo, counts):
        # duo contient (movie_id, user_id)
        # On additionne le nombre de tags pour ce duo précis
        yield duo, sum(counts)

if __name__ == '__main__':
    TagsDuo.run()
```

#### On lance le script Python
```
python tags_duo.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar ml-25m/tags.csv > res.txt
```


#### On visualise le contenu du resultat

```
[maria_dev@sandbox-hdp ~]$ head -20 res.txt

["1", "100538"] 4
["1", "10231"]  2
["1", "102568"] 4
["1", "102901"] 1
["1", "103368"] 1
["1", "103371"] 1
["1", "103883"] 3
["1", "104394"] 9
["1", "1048"]   1
["1", "105717"] 1
["1", "105809"] 5
["1", "107432"] 2
["1", "109146"] 2
["1", "109258"] 1
["1", "110339"] 3
["1", "110966"] 1
["1", "111033"] 3
["1", "111139"] 1
["1", "111183"] 1
["1", "112824"] 3
```
