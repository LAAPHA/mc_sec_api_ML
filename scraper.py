import joblib
from time import sleep
import datetime
import re
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import numpy as np

liste_liens1 = ['bank','mortgage_broker','travel_insurance_company','distance_learning_center']
liste_liens2 = ['All','bank','mortgage_broker','travel_insurance_company','distance_learning_center']
categorie, pays ,marque, liens_marque, reviews = [],[],[],[],[]

# url = "https://www.trustpilot.com/categories"

def fonction_scraper(url):

    if url:
            
            for lien_cc in liste_liens1:
                        
                lien = str(url) + '/' + str(lien_cc) +'?country=FR'
                
                # récupératu du code html de toute la page et le stocker dans une variable: soup
                page = requests.get(lien, verify = False)
                soup = bs(page.content, "lxml")
                
                # # Sélectionner la partie de la page qui contient les numéros de page
                # pagination_div = soup.find('nav', class_='pagination_pagination___F1qS')

                # # Extraire les numéros de page en parcourant les éléments de la pagination
                # try:
                #   page_numbers = []
                #   for item in pagination_div.find_all(['span']):
                #       page_numbers.append(item.get_text())
                # except:
                #       page_numbers.append(1)
                # # print(page_numbers)
                # nb_pages = int(page_numbers[-2])
                # print(lien_cc,'contient :',nb_pages, 'pages!')

                ### début de la boucle qui parcours les pages d'une marque X
                for X in range(1,2+1):

                    sleep(0) # attendre une demi seconde entre chaque page, pas obligé
                    lien2 = str(url)+'/'+str(lien_cc)+'?country=FR&page='+str(X)
                    
                    # récupératu du code html de toute la page et le stocker dans une variable: soup
                    page = requests.get(lien2, verify = False)
                    soup2 = bs(page.content, "lxml")
                    soup_marques = soup2.find_all('div', class_ = ("paper_paper__1PY90 paper_outline__lwsUX card_card__lQWDv card_noPadding__D8PcU styles_wrapper__2JOo2"))
                    # print(soup_marques)
                    # company = soup.find('h1',class_='typography_default__hIMlQ typography_appearance-default__AAY17 title_title__i9V__').text.strip() ## récupérer le nom de la marque

                    ## parcourir le code html (soup) pour extraire les informations des balises
                    for lien_m in soup_marques:
                            # lienss =
                            marque.append(lien_m.find('p',class_ ='typography_heading-xs__jSwUz typography_appearance-default__AAY17 styles_displayName__GOhL2').text)
                            liens_marque.append(lien_m.find('a',class_ ='link_internal__7XN06 link_wrapper__5ZJEx styles_linkWrapper__UWs5j').get('href'))
                            reviews.append(lien_m.find('p',class_ ='typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_ratingText__yQ5S7'))
                            # reviews.append(lien_m.find('div',class_ ='styles_rating__pY5Pk'))
                            categorie.append(lien_cc)
                            pays.append("FR")


            data = {
                    'marque': marque,
                    'liens_marque': liens_marque,
                    'categorie': categorie,
                    'reviews': reviews,
                    'pays': pays
                    }

            # création de Dataframe pour y stocker les données
            df_liens = pd.DataFrame(data)

            ###############  data cleaning: ############################
            df_liens['liens_marque'] = df_liens['liens_marque'].str.replace('/review/','')

            ## extraire le nombre de reviews en utilisant une fonction
            def extraire_chiffres(texte):
                pattern = r'\|\</span>([0-9,]+)'
                match = re.search(pattern, str(texte))
                if match:
                    chiffres_apres_barre_span = match.group(1)
                    return chiffres_apres_barre_span
                elif len(str(texte)) < 8:
                    return texte
                else:
                    return None
            ## appliquer la fonction à la colonne reviews
            df_liens['reviews'] = df_liens['reviews'].apply(extraire_chiffres)
            ## convertir reviews en nombre
            df_liens['reviews'] = df_liens['reviews'].str.replace(',','')
            # df_liens['reviews'] = df_liens['reviews'].str.replace('None',0)
            df_liens['reviews']=df_liens['reviews'].astype(float)
            ## trier le dataframe
            df_liens = df_liens.sort_values(by=['categorie', 'reviews'], ascending=[True, False])

            ## enregistrer le dataframe traité en csv et excel: ne pas oublier de modififier _liste_liens4 pour ne pas ecraser l'ancien enregistrement
            df_liens.to_csv('datas/Avis_trustpilot_liste_liens.csv')
            df_liens.to_excel('datas/Avis_trustpilot_liste_liens.xlsx')

            ############### fin webscraping de la liste des liens #################################################
            
            #region webscraping tous liens

###################### debut de webscraping: de tous les liens-------------------------------------------------

################################################ bouton Scraper tous les avis##############################################
                # if st.button("Scraper tous les avis"):

           
            print("scraper les avis clients: opération encours.... ")
            # st.dataframe(df_liste_liens.head(10))
            # Obtenez la date et l'heure actuelles
            
            date_actuelle = datetime.datetime.now()
            # Affichez la date uniquement au format "Année-Mois-Jour"
            # st.write("Date du jour (format court) :", date_actuelle.strftime("%d-%m-%Y"))
            
            ## le dataframe de travail, choix des catégories à scraper: pour scraper en plusieurs morceaux
            
            # df_liens_filtré = df_liens.loc[(df_liens.head(2)) ]

            # if lien_c == 'All':
            #     liste_f = liste_liens1
            # else:
            #     liste_f = [lien_c]


            # df_liens_filtré = df_liens.loc[(df_liens['categorie']=='travel_insurance_company') ]
            df_liens_filtré = df_liens

               
            nombre_fichier = 0

            Data = {}   # désactiver cette ligne pour enregistrer un fichier par catégorie
            tout , noms, date_commentaire, date_experience, notes, titre_com, companies, reponses = [],[],[],[],[],[],[],[]
            commentaire, verified ,test, site,nombre_pages , date_scrap, date_reponse, date_rep,categorie_bis = [],[],[],[],[],[],[],[],[]

            for lien_cat in liste_liens1:
                ## parcourir la liste des liens pour plusieurs catégories

                # Data = {}  # activer cette ligne pour enregistrer un fichier par catégorie!!

                # création des  listes vides
                

                df_marque = df_liens_filtré.loc[df_liens_filtré ['categorie'] == lien_cat]

                for lien_c in df_marque['liens_marque']:
                    
                    lien = 'https://www.trustpilot.com/review/'+str(lien_c)+'?page=1'

                    # récupératu du code html de toute la page et le stocker dans une variable: soup
                    try:
                        page = requests.get(lien, verify = False)
                        soup = bs(page.content, "lxml")
                    except:
                        # st.write(f"Une exception s'est produite pour {lien_c}: {e}")
                        print(f"Une exception s'est produite pour {lien_c}:")
                        # continue

                    # Sélectionner la partie de la page qui contient les numéros de page
                    pagination_div = soup.find('div', class_='styles_pagination__6VmQv')
                    # Extraire les numéros de page en parcourant les éléments de la pagination
                    page_numbers = []

                    try:
                        for item in pagination_div.find_all(['span']):
                            page_numbers.append(item.get_text())

                        # print(page_numbers)
                        nb_pages = int(page_numbers[-2])
                        # st.write(lien_c, 'contient : ',nb_pages, ' pages!')
                    except:
                        nb_pages = 1

                    ### début de la boucle qui parcours les pages d'une marque X
                    # for X in range(1,nb_pages+1):
                    
                    for X in range(1,2):

                        # sleep(0) # attendre une demi seconde entre chaque page, pas obligé

                        lien = 'https://www.trustpilot.com/review/'+str(lien_c)+'?page='+str(X)
                        # récupératu du code html de toute la page et le stocker dans une variable: soup
                        page = requests.get(lien, verify = False)
                        soup = bs(page.content, "lxml")
                        # print(soup.prettify())
                        avis_clients = soup.find_all('div', attrs = {'class': "styles_cardWrapper__LcCPA styles_show__HUXRb styles_reviewCard__9HxJJ"})

                        try:
                            company = soup.find('h1',class_='typography_default__hIMlQ typography_appearance-default__AAY17 title_title__i9V__').text.strip() ## récupérer le nom de la marque
                        except:
                            company = None
                        ## parcourir le code html (soup) pour extraire les informations des balises
                        for avis in avis_clients:

                            # tout.append(avis.find('div',class_='styles_reviewContent__0Q2Tg').text.strip())
                            try:
                                noms.append(avis.find('span',class_='typography_heading-xxs__QKBS8 typography_appearance-default__AAY17').text.strip())
                            except:
                                noms.append(None)
                            try:
                                titre_com.append(avis.find('h2',class_='typography_heading-s__f7029 typography_appearance-default__AAY17').text.strip())
                            except:
                                titre_com.append(None)
                            try:
                                commentaire.append(avis.find('p').text.strip())
                            except:
                                commentaire.append(None)
                            try:
                                reponses.append(avis.find('p',class_='typography_body-m__xgxZ_ typography_appearance-default__AAY17 styles_message__shHhX'))
                            except:
                                reponses.append(None)
                            # try:
                            # notes.append(avis.find('img')['alt'])
                            # except:
                            #   notes.append(None)
                            try:
                                notes.append(avis.find('div',class_='star-rating_starRating__4rrcf star-rating_medium__iN6Ty'))
                            except:
                                notes.append(None)
                            try:
                                date_experience.append(avis.find('p',class_='typography_body-m__xgxZ_ typography_appearance-default__AAY17').text.strip())
                            except:
                                date_experience.append(None)
                            try:
                                date_commentaire.append(avis.find('div',class_='styles_reviewHeader__iU9Px').text.strip())
                            except:
                                date_commentaire.append(None)
                            # try:
                            # date_reponse.append(avis.find('div',class_='styles_content__Hl2Mi'))
                            # except:
                            #   noms.append(None)
                            try:
                                companies.append(company)
                            except:
                                companies.append(None)
                            try:
                                site.append(lien)
                            except:
                                site.append(None)
                            try:
                                nombre_pages.append(nb_pages)
                            except:
                                nombre_pages.append(None)
                            try:
                                categorie_bis.append(lien_cat)
                            except:
                                categorie_bis.append(None)

                            date_scrap.append(date_actuelle.strftime("%d-%m-%Y"))

                    nombre_fichier+=1

                    # st.write('Nous avons scrapé ' ,nb_pages, ' pages du site: ', lien_cat , '/',lien_c, ' *** N°:***',nombre_fichier)


                print('!!!!!!!!!!!!!!!!!La categorie !!' , lien_cat, '!! est scrapée et enregistrée en Excel!!!!!!!!!!!!!!')
            
            # création d'un dictionnaire avec les listes crées précédement
            data = {
                        'categorie_bis': categorie_bis,
                        'companies': companies,
                        'noms': noms,
                        'titre_com': titre_com,
                        'commentaire': commentaire,
                        'reponses': reponses,
                        'notes': notes,
                        'date_experience': date_experience,
                        'date_commentaire': date_commentaire,
                        # 'date_reponse': date_reponse,
                        'site': site,
                        'nombre_pages': nombre_pages,
                        'date_scrap': date_scrap
                    }

                # création de Dataframe pour y stocker les données
                # df = pd.DataFrame(data)
                # enregistrer le dataframe dans un fichier .csv

                # df.to_excel('Avis_trustpilot_supply_chain_brut_'+str(lien_cat)+'.xlsx')
                # df.to_excel('Avis_trustpilot_supply_chain_brut_total.xlsx')

                

            # création de Dataframe pour y stocker les données
            df = pd.DataFrame(data)

            df.to_excel('datas/Avis_trustpilot_supply_chain_brut_total.xlsx')
            df.to_csv('datas/Avis_trustpilot_supply_chain_brut_total.csv')

            print("------------------------------------------------------------------------")
            print('#### Résultats: données brutes scrapées:')
            
            print("Voici le Dataframe des données brutes scrapée (données non traitées). \nD'après ce que nous voyons ci-dessus, Les données scrapées nécessitent un traitement supplémentaire avec text mining. Nous allons aussi procéder à la création de nouvelles features engineering.")
            
            # df.head(10)

            
            print(df['categorie_bis'].value_counts())
            print("La taille du df brut: ",df.shape)

            # st.dataframe(df_liste_liens.describe())
            # if st.checkbox("Afficher les NA") :
            #   st.dataframe(df_liste_liens.isna().sum())



    else:
        print("Veuillez saisir une URL valide.")

## lancer la fonction
url = "https://www.trustpilot.com/categories"
fonction_scraper(url)