https://github.com/Thib0727/BigDataCC2-Thollet-Muller


## PREPARATION

#### Décompresser le fichier
unzip ml-25m.zip

#### verification
ls -l

### Envoi sur hdfs 
hdfs dfs -put ml-25m/tags.csv

#### On envoie aussi le fichier de petite taille

### Envoi vers la VM 

#### Dans un nouveau cmd on execute
scp -P 2222 "C:\Users\ythollet\Downloads\echantillon-tags.csv" maria_dev@localhost:~
### Envoi sur hdfs 
hdfs dfs -put echantillon-tags.csv


## 1- Combien de tags chaque film possède-t-il ?
```
nano tags_per_movie.py
```
On copie-colle le code python
-*- coding: utf-8 -*-
```
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

1. Analyse des volumes (Compteurs Map-Reduce)
L'examen des compteurs de volume permet de valider la rigueur algorithmique du script. Sur un total de 1 093 361 lignes lues (Map input records), le Mapper a généré exactement 1 093 360 couples clé-valeur. Cet écart d'une unité confirme que la condition de filtrage a correctement identifié et exclu la ligne d'en-tête CSV. La phase de réduction a ensuite agrégé ce million de lignes en 45 251 groupes uniques (Reduce input groups), ce qui correspond au nombre précis de films ayant reçu au moins une évaluation. Cette réduction drastique du volume de données (de ~1M à ~45k lignes) illustre parfaitement l'efficacité du paradigme MapReduce pour l'agrégation de données massives.

2. Performance et Parallélisme
L'analyse des logs révèle une exploitation optimale des mécanismes de distribution de Hadoop. Le framework a segmenté le fichier d'entrée en deux blocs distincts (Launched map tasks = 2), permettant un traitement parallèle qui divise théoriquement le temps d'exécution. Un point crucial est la confirmation du "Data Locality" pour l'intégralité des tâches : Hadoop a exécuté le code Python directement sur les nœuds stockant physiquement les blocs de données, supprimant ainsi tout goulot d'étranglement lié au transfert réseau. Avec un temps de traitement cumulé d'environ 30 secondes pour plus d'un million d'enregistrements, les performances sont excellentes pour un environnement Sandbox, démontrant la faible latence du moteur YARN lors de l'allocation des ressources.


## 2- Combien de tags chaque utilisateur a-t-il ajoutés ?
```
nano tags_per_user.py
```

#### On copie-colle le code python
#### -*- coding: utf-8 -*-
```
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

2. Analyse des volumes (Compteurs Map-Reduce)
Le job a traité avec succès l'intégralité du fichier source, comme en témoignent les 1 093 361 lignes lues (Map input records). Le Mapper a correctement filtré l'en-tête pour ne générer que 1 093 360 enregistrements utiles. La phase de réduction a permis d'identifier 14 592 utilisateurs uniques (Reduce input groups), consolidant ainsi l'ensemble des tags émis dans une liste synthétique. La précision des compteurs de sortie (Reduce output records) correspondant exactement aux groupes d'entrée confirme l'intégrité du processus d'agrégation distribuée.

3. Performance et Parallélisme
Sur le plan infrastructurel, le job a profité d'un parallélisme de niveau 2 (Launched map tasks), optimisant le temps de traitement global sur les deux blocs de données HDFS. L'efficacité du cluster est soulignée par le score de 100% en localité de données (Data-local map tasks), évitant ainsi toute latence liée au réseau lors de la lecture. Le temps de calcul total, réparti entre 17,8s pour le mapping et 8,9s pour le reducing, démontre la capacité de Hadoop à traiter des volumes dépassant le million de lignes en moins de trente secondes, tout en assurant un nettoyage automatique des ressources temporaires après l'extraction finale vers le fichier res.txt.
