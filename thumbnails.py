import os
import aiohttp
import traceback
from PIL import Image ,ImageDraw ,ImageFilter ,ImageFont ,ImageEnhance ,ImageOps
from yt_dlp import YoutubeDL
from pathlib import Path
from io import BytesIO
import unicodedata
import logging
import shutil
import re
import config
ITALIC_TO_REGULAR =str .maketrans ({119860 :'A',119861 :'B',119862 :'C',119863 :'D',119864 :'E',119865 :'F',119866 :'G',119867 :'H',119868 :'I',119869 :'J',119870 :'K',119871 :'L',119872 :'M',119873 :'N',119874 :'O',119875 :'P',119876 :'Q',119877 :'R',119878 :'S',119879 :'T',119880 :'U',119881 :'V',119882 :'W',119883 :'X',119884 :'Y',119885 :'Z',119886 :'a',119887 :'b',119888 :'c',119889 :'d',119890 :'e',119891 :'f',119892 :'g',119893 :'h',119894 :'i',119895 :'j',119896 :'k',119897 :'l',119898 :'m',119899 :'n',119900 :'o',119901 :'p',119902 :'q',119903 :'r',119904 :'s',119905 :'t',119906 :'u',119907 :'v',119908 :'w',119909 :'x',119910 :'y',119911 :'z',120328 :'A',120329 :'B',120330 :'C',120331 :'D',120332 :'E',120333 :'F',120334 :'G',120335 :'H',120336 :'I',120337 :'J',120338 :'K',120339 :'L',120340 :'M',120341 :'N',120342 :'O',120343 :'P',120344 :'Q',120345 :'R',120346 :'S',120347 :'T',120348 :'U',120349 :'V',120350 :'W',120351 :'X',120352 :'Y',120353 :'Z',120354 :'a',120355 :'b',120356 :'c',120357 :'d',120358 :'e',120359 :'f',120360 :'g',120361 :'h',120362 :'i',120363 :'j',120364 :'k',120365 :'l',120366 :'m',120367 :'n',120368 :'o',120369 :'p',120370 :'q',120371 :'r',120372 :'s',120373 :'t',120374 :'u',120375 :'v',120376 :'w',120377 :'x',120378 :'y',120379 :'z',120380 :'A',120381 :'B',120382 :'C',120383 :'D',120384 :'E',120385 :'F',120386 :'G',120387 :'H',120388 :'I',120389 :'J',120390 :'K',120391 :'L',120392 :'M',120393 :'N',120394 :'O',120395 :'P',120396 :'Q',120397 :'R',120398 :'S',120399 :'T',120400 :'U',120401 :'V',120402 :'W',120403 :'X',120404 :'Y',120405 :'Z',120406 :'a',120407 :'b',120408 :'c',120409 :'d',120410 :'e',120411 :'f',120412 :'g',120413 :'h',120414 :'i',120415 :'j',120416 :'k',120417 :'l',120418 :'m',120419 :'n',120420 :'o',120421 :'p',120422 :'q',120423 :'r',120424 :'s',120425 :'t',120426 :'u',120427 :'v',120428 :'w',120429 :'x',120430 :'y',120431 :'z'})

def convert_italic_unicode (text ):
    return text .translate (ITALIC_TO_REGULAR )

def remove_emojis (text ):

    if not text :
        return text

    cleaned =''
    for char in text :
        try :

            codepoint =ord (char )

            if codepoint >=0x1F000 :
                continue
            if 0x2600 <=codepoint <=0x27BF :
                continue
            if 0x1F300 <=codepoint <=0x1F9FF :
                continue

            category =unicodedata .category (char )
            if category .startswith ('C'):
                continue
            cleaned +=char
        except :
            continue
    return cleaned .strip ()
ASSETS_DIR = Path('fonts')
ARM_FONTS =['NotoSansArmenian-Regular.ttf','GHEA_Grapalat.ttf','GHEA_Mariam.ttf','Sylfaen.ttf','DejaVuSans.ttf','ArianAMU-Regular.ttf','Webstart.ttf','FreeSans.ttf','Tarumian-Ando-Regular.ttf','LinotypeMaralArmenian.ttf','NotoSansArmenian-Bold.ttf','DejaVuSans-Bold.ttf','ArianAMU-Bold.ttf','Webstart-Bold.ttf','FreeSans-Bold.ttf','Tarumian-Ando-Bold.ttf','NotoSansArmenian-Italic.ttf','NotoSansArmenianMono-Regular.ttf','DejaVuSansMono-Regular.ttf','DejaVuSansMono-Bold.ttf']
GLOBAL_FONTS =['NotoSans-Regular.ttf','NotoSans-Bold.ttf','NotoSans-Italic.ttf','NotoSansMono-Regular.ttf','NotoSansMono-Bold.ttf','NotoSansSymbols-Regular.ttf','NotoSansSymbols-Bold.ttf','NotoSansSymbols2-Regular.ttf','NotoSansSymbols2-Bold.ttf','NotoSansMath-Regular.ttf','NotoMusic-Regular.ttf','NotoSansAdlam-Regular.ttf','NotoSansAdlam-Bold.ttf','NotoSansAdlamUnjoined-Regular.ttf','NotoSansAdlamUnjoined-Bold.ttf','NotoSansAnatolianHieroglyphs-Regular.ttf','NotoSansArabic-Regular.ttf','NotoSansArabic-Bold.ttf','NotoSansArabicUI-Regular.ttf','NotoSansArabicUI-Bold.ttf','NotoKufiArabic-Regular.ttf','NotoKufiArabic-Bold.ttf','NotoNaskhArabic-Regular.ttf','NotoNaskhArabic-Bold.ttf','NotoNaskhArabicUI-Regular.ttf','NotoNaskhArabicUI-Bold.ttf','NotoNastaliqUrdu-Regular.ttf','NotoNastaliqUrdu-Bold.ttf','NotoSansArmenian-Regular.ttf','NotoSansArmenian-Bold.ttf','NotoSansAvestan-Regular.ttf','NotoSansBalinese-Regular.ttf','NotoSansBalinese-Bold.ttf','NotoSansBamum-Regular.ttf','NotoSansBamum-Bold.ttf','NotoSansBassaVah-Regular.ttf','NotoSansBassaVah-Bold.ttf','NotoSansBatak-Regular.ttf','NotoSansBengali-Regular.ttf','NotoSansBengali-Bold.ttf','NotoSansBengaliUI-Regular.ttf','NotoSansBengaliUI-Bold.ttf','NotoSansBhaiksuki-Regular.ttf','NotoSansBrahmi-Regular.ttf','NotoSansBuginese-Regular.ttf','NotoSansBuhid-Regular.ttf','NotoSansCanadianAboriginal-Regular.ttf','NotoSansCanadianAboriginal-Bold.ttf','NotoSansCarian-Regular.ttf','NotoSansCaucasianAlbanian-Regular.ttf','NotoSansChakma-Regular.ttf','NotoSansCham-Regular.ttf','NotoSansCham-Bold.ttf','NotoSansCherokee-Regular.ttf','NotoSansCherokee-Bold.ttf','NotoSansChorasmian-Regular.ttf','NotoSansCoptic-Regular.ttf','NotoSansCuneiform-Regular.ttf','NotoSansCypriot-Regular.ttf','NotoSansCyproMinoan-Regular.ttf','NotoSansDeseret-Regular.ttf','NotoSansDevanagari-Regular.ttf','NotoSansDevanagari-Bold.ttf','NotoSansDevanagariUI-Regular.ttf','NotoSansDevanagariUI-Bold.ttf','NotoSansDuployan-Regular.ttf','NotoSansDuployan-Bold.ttf','NotoSansEgyptianHieroglyphs-Regular.ttf','NotoSansElbasan-Regular.ttf','NotoSansElymaic-Regular.ttf','NotoSansEthiopic-Regular.ttf','NotoSansEthiopic-Bold.ttf','NotoSansGeorgian-Regular.ttf','NotoSansGeorgian-Bold.ttf','NotoSansGlagolitic-Regular.ttf','NotoSansGothic-Regular.ttf','NotoSansGrantha-Regular.ttf','NotoSansGujarati-Regular.ttf','NotoSansGujarati-Bold.ttf','NotoSansGujaratiUI-Regular.ttf','NotoSansGujaratiUI-Bold.ttf','NotoSansGunjalaGondi-Regular.ttf','NotoSansGunjalaGondi-Bold.ttf','NotoSansGurmukhi-Regular.ttf','NotoSansGurmukhi-Bold.ttf','NotoSansGurmukhiUI-Regular.ttf','NotoSansGurmukhiUI-Bold.ttf','NotoSansHanifiRohingya-Regular.ttf','NotoSansHanifiRohingya-Bold.ttf','NotoSansHanunoo-Regular.ttf','NotoSansHatran-Regular.ttf','NotoSansHebrew-Regular.ttf','NotoSansHebrew-Bold.ttf','NotoRashiHebrew-Regular.ttf','NotoRashiHebrew-Bold.ttf','NotoSansImperialAramaic-Regular.ttf','NotoSansIndicSiyaqNumbers-Regular.ttf','NotoSansInscriptionalPahlavi-Regular.ttf','NotoSansInscriptionalParthian-Regular.ttf','NotoSansJavanese-Regular.ttf','NotoSansJavanese-Bold.ttf','NotoSansKaithi-Regular.ttf','NotoSansKannada-Regular.ttf','NotoSansKannada-Bold.ttf','NotoSansKannadaUI-Regular.ttf','NotoSansKannadaUI-Bold.ttf','NotoSansKawi-Regular.ttf','NotoSansKawi-Bold.ttf','NotoSansKayahLi-Regular.ttf','NotoSansKayahLi-Bold.ttf','NotoSansKharoshthi-Regular.ttf','NotoSansKhmer-Regular.ttf','NotoSansKhmer-Bold.ttf','NotoSansKhmerUI-Regular.ttf','NotoSansKhmerUI-Bold.ttf','NotoSansKhojki-Regular.ttf','NotoSansKhudawadi-Regular.ttf','NotoSansLao-Regular.ttf','NotoSansLao-Bold.ttf','NotoSansLaoLooped-Regular.ttf','NotoSansLaoLooped-Bold.ttf','NotoSansLaoUI-Regular.ttf','NotoSansLepcha-Regular.ttf','NotoSansLimbu-Regular.ttf','NotoSansLinearA-Regular.ttf','NotoSansLinearB-Regular.ttf','NotoSansLisu-Regular.ttf','NotoSansLisu-Bold.ttf','NotoSansLycian-Regular.ttf','NotoSansLydian-Regular.ttf','NotoSansMahajani-Regular.ttf','NotoSansMalayalam-Regular.ttf','NotoSansMalayalam-Bold.ttf','NotoSansMalayalamUI-Regular.ttf','NotoSansMalayalamUI-Bold.ttf','NotoSansMandaic-Regular.ttf','NotoSansManichaean-Regular.ttf','NotoSansMarchen-Regular.ttf','NotoSansMasaramGondi-Regular.ttf','NotoSansMayanNumerals-Regular.ttf','NotoSansMedefaidrin-Regular.ttf','NotoSansMedefaidrin-Bold.ttf','NotoSansMeeteiMayek-Regular.ttf','NotoSansMeeteiMayek-Bold.ttf','NotoSansMendeKikakui-Regular.ttf','NotoSansMeroitic-Regular.ttf','NotoSansMiao-Regular.ttf','NotoSansModi-Regular.ttf','NotoSansMongolian-Regular.ttf','NotoSansMro-Regular.ttf','NotoSansMultani-Regular.ttf','NotoSansMyanmar-Regular.ttf','NotoSansMyanmar-Bold.ttf','NotoSansMyanmarUI-Regular.ttf','NotoSansMyanmarUI-Bold.ttf','NotoSansNabataean-Regular.ttf','NotoSansNagMundari-Regular.ttf','NotoSansNagMundari-Bold.ttf','NotoSansNandinagarian-Regular.ttf','NotoSansNewTaiLue-Regular.ttf','NotoSansNewTaiLue-Bold.ttf','NotoSansNewa-Regular.ttf','NotoSansNushu-Regular.ttf','NotoSansNyiakengPuachueHmong-Regular.ttf','NotoSansOgham-Regular.ttf','NotoSansOlChiki-Regular.ttf','NotoSansOldHungarian-Regular.ttf','NotoSansOldItalic-Regular.ttf','NotoSansOldNorthArabian-Regular.ttf','NotoSansOldPermic-Regular.ttf','NotoSansOldSogdian-Regular.ttf','NotoSansOldTurkic-Regular.ttf','NotoSansOriya-Regular.ttf','NotoSansOriya-Bold.ttf','NotoSansOriyaUI-Regular.ttf','NotoSansOriyaUI-Bold.ttf','NotoSansOsage-Regular.ttf','NotoSansOsmanya-Regular.ttf','NotoSansPahawhHmong-Regular.ttf','NotoSansPalmyrene-Regular.ttf','NotoSansPauCinHau-Regular.ttf','NotoSansPhagsPa-Regular.ttf','NotoSansPhoenician-Regular.ttf','NotoSansPhoneticExtensions-Regular.ttf','NotoSansPsalterPahlavi-Regular.ttf','NotoSansRejang-Regular.ttf','NotoSansRunic-Regular.ttf','NotoSansSamaritan-Regular.ttf','NotoSansSaurashtra-Regular.ttf','NotoSansSharda-Regular.ttf','NotoSansShavian-Regular.ttf','NotoSansSharada-Regular.ttf','NotoSansSiddham-Regular.ttf','NotoSansSinhala-Regular.ttf','NotoSansSinhala-Bold.ttf','NotoSansSogdian-Regular.ttf','NotoSansSoraSompeng-Regular.ttf','NotoSansSoyombo-Regular.ttf','NotoSansSundanese-Regular.ttf','NotoSansSundanese-Bold.ttf','NotoSansSignWriting-Regular.ttf','NotoSansSylotiNagri-Regular.ttf','NotoSansSyriacEastern-Regular.ttf','NotoSansSyriacEastern-Bold.ttf','NotoSansSyriacEstrangela-Regular.ttf','NotoSansSyriacEstrangela-Bold.ttf','NotoSansSyriacWestern-Regular.ttf','NotoSansSyriacWestern-Bold.ttf','NotoSansTagalog-Regular.ttf','NotoSansTagbanwa-Regular.ttf','NotoSansTaiLe-Regular.ttf','NotoSansTaiTham-Regular.ttf','NotoSansTaiTham-Bold.ttf','NotoSansTaiViet-Regular.ttf','NotoSansTakri-Regular.ttf','NotoSansTamil-Regular.ttf','NotoSansTamil-Bold.ttf','NotoSansTamilUI-Regular.ttf','NotoSansTamilUI-Bold.ttf','NotoSansTangut-Regular.ttf','NotoSansTelugu-Regular.ttf','NotoSansTelugu-Bold.ttf','NotoSansTeluguUI-Regular.ttf','NotoSansTeluguUI-Bold.ttf','NotoSansThaana-Regular.ttf','NotoSansThaana-Bold.ttf','NotoSansThai-Regular.ttf','NotoSansThai-Bold.ttf','NotoSansThaiUI-Regular.ttf','NotoSansThaiUI-Bold.ttf','NotoSansTibetan-Regular.ttf','NotoSansTibetan-Bold.ttf','NotoSansTifinagh-Regular.ttf','NotoSansTiro-Regular.ttf','NotoSansUgaritic-Regular.ttf','NotoSansVai-Regular.ttf','NotoSansVai-Bold.ttf','NotoSansVithkuqi-Regular.ttf','NotoSansWancho-Regular.ttf','NotoSansWarangCiti-Regular.ttf','NotoSansYeziCorbit-Regular.ttf','NotoSansYi-Regular.ttf','NotoSansYi-Bold.ttf','NotoSansZanabazarSquare-Regular.ttf','NotoColorEmoji.ttf','NotoSansCJK-Regular.ttc','NotoSansCJK-Bold.ttc','STIXTwoMath-Regular.otf','FiraMath-Regular.otf','NotoSansDevanagari-Bold.ttf','NotoSansArabic-Bold.ttf','GoNotoCurrent-Regular.ttf','GoNotoCurrent-Bold.ttf','GoNotoCurrentSerif.ttf','GoNotoAncient.ttf','GoNotoKurrent-Regular.ttf']
ALL_FONTS =GLOBAL_FONTS +ARM_FONTS
FONTS ={'regular':[],'bold':[],'italic':[],'mono':[]}

def get_script_name (code ):
    script_map ={'Cyrl':'Cyrillic','Armn':'Armenian','Arab':'Arabic','Beng':'Bengali','Deva':'Devanagari','Ethi':'Ethiopic','Geor':'Georgian','Grek':'Greek','Gujr':'Gujarati','Hebr':'Hebrew','Knda':'Kannada','Khmr':'Khmer','Laoo':'Lao','Mlym':'Malayalam','Mong':'Mongolian','Mymr':'Myanmar','Orya':'Oriya','Sinh':'Sinhala','Taml':'Tamil','Telu':'Telugu','Thaa':'Thaana','Thai':'Thai','Tibt':'Tibetan','Cans':'CanadianAboriginal','Cher':'Cherokee','Copt':'Coptic','Cprt':'Cypriot','Dsrt':'Deseret','Egyp':'EgyptianHieroglyphs','Ethi':'Ethiopic','Goth':'Gothic','Hani':'Han','Hira':'Hiragana','Kana':'Katakana','Khar':'Kharoshthi','Limb':'Limbu','Linb':'LinearB','Lisu':'Lisu','Lyci':'Lycian','Lydi':'Lydian','Mand':'Mandaic','Mani':'Manichaean','Maya':'MayanNumerals','Mend':'MendeKikakui','Merc':'Meroitic','Nkoo':'Nko','Ogam':'Ogham','Olck':'OlChiki','Perm':'OldPermic','Phli':'InscriptionalPahlavi','Phlv':'PsalterPahlavi','Phnx':'Phoenician','Plrd':'Miao','Roro':'Rongorongo','Runr':'Runic','Samr':'Samaritan','Sara':'Saurashtra','Saur':'Saurashtra','Shrd':'Sharada','Sund':'Sundanese','Syrc':'Syriac','Tagb':'Tagbanwa','Tale':'TaiLe','Talu':'NewTaiLue','Taml':'Tamil','Tavt':'TaiViet','Tfng':'Tifinagh','Tglg':'Tagalog','Thaa':'Thaana','Thai':'Thai','Tibt':'Tibetan','Ugar':'Ugaritic','Vaii':'Vai','Yiii':'Yi','Zinh':'Inherited','Zyyy':'Common','Zzzz':'Unknown','Adlm':'Adlam','Aghb':'CaucasianAlbanian','Ahom':'Ahom','Armi':'ImperialAramaic','Avst':'Avestan','Bali':'Balinese','Bamu':'Bamum','Bass':'BassaVah','Batk':'Batak','Bhaiksuki':'Bhaiksuki','Bopo':'Bopomofo','Brahmi':'Brahmi','Bugi':'Buginese','Buhid':'Buhid','Cakm':'Chakma','Cans':'CanadianAboriginal','Cari':'Carian','Cham':'Cham','Cherokee':'Cherokee','Chorasmian':'Chorasmian','Copt':'Coptic','Cprt':'Cypriot','CyproMinoan':'CyproMinoan','Dsrt':'Deseret','Duployan':'Duployan','Egyp':'EgyptianHieroglyphs','Elbasan':'Elbasan','Elymaic':'Elymaic','Geok':'Khutsuri','Glagolitic':'Glagolitic','Gothic':'Gothic','Grantha':'Grantha','Gujarati':'Gujarati','GunjalaGondi':'GunjalaGondi','Gurmukhi':'Gurmukhi','Hani':'Han','Hangul':'Hangul','Hatran':'Hatran','Hebrew':'Hebrew','AnatolianHieroglyphs':'AnatolianHieroglyphs','PahawhHmong':'PahawhHmong','NyiakengPuachueHmong':'NyiakengPuachueHmong','OldHungarian':'OldHungarian','OldItalic':'OldItalic','Javanese':'Javanese','Kaithi':'Kaithi','Kannada':'Kannada','KayahLi':'KayahLi','Kharoshthi':'Kharoshthi','Khmer':'Khmer','Khojki':'Khojki','Limbu':'Limbu','LinearA':'LinearA','LinearB':'LinearB','Lisu':'Lisu','Loma':'Loma','Lycian':'Lycian','Lydian':'Lydian','Mahajani':'Mahajani','Makasar':'Makasar','Manichaean':'Manichaean','Marchen':'Marchen','MeeteiMayek':'MeeteiMayek','Multani':'Multani','Myanmar':'Myanmar','Nabataean':'Nabataean','Nandinagari':'Nandinagari','OldNorthArabian':'OldNorthArabian','Naskh':'Naskh','Nushu':'Nushu','Nko':'Nko','Ogham':'Ogham','OlChiki':'OlChiki','Palmyrene':'Palmyrene','OldPermic':'OldPermic','PhagsPa':'PhagsPa','InscriptionalPahlavi':'InscriptionalPahlavi','PsalterPahlavi':'PsalterPahlavi','InscriptionalParthian':'InscriptionalParthian','Phoenician':'Phoenician','Miao':'Miao','OldSogdian':'OldSogdian','Rejang':'Rejang','Rongorongo':'Rongorongo','Samaritan':'Samaritan','Saurashtra':'Saurashtra','SignWriting':'SignWriting','Shavian':'Shavian','Sharada':'Sharada','Siddham':'Siddham','Khudawadi':'Khudawadi','Sinhala':'Sinhala','Sogdian':'Sogdian','SoraSompeng':'SoraSompeng','Soyombo':'Soyombo','Sundanese':'Sundanese','SylotiNagri':'SylotiNagri','Syriac':'Syriac','Tagbanwa':'Tagbanwa','Takri':'Takri','TaiLe':'TaiLe','NewTaiLue':'NewTaiLue','Tamil':'Tamil','Tangut':'Tangut','TaiViet':'TaiViet','Telugu':'Telugu','Thaana':'Thaana','Thai':'Thai','Tibetan':'Tibetan','Tifinagh':'Tifinagh','Tirhuta':'Tirhuta','Ugaritic':'Ugaritic','Vai':'Vai','VisibleSpeech':'VisibleSpeech','WarangCiti':'WarangCiti','Wancho':'Wancho','Yi':'Yi','ZanabazarSquare':'ZanabazarSquare','Inherited':'Inherited','MathematicalNotation':'MathematicalNotation'}
    return script_map .get (code ,'Common')

def classify_font_style (name ):
    name_lower =name .lower ()
    if any ((word in name_lower for word in ['bold','heavy','black'])):
        return 'bold'
    elif any ((word in name_lower for word in ['italic','oblique'])):
        return 'italic'
    elif any ((word in name_lower for word in ['mono','code','fixed'])):
        return 'mono'
    elif 'serif'in name_lower :
        return 'regular'
    elif 'ancient'in name_lower or 'kurrent'in name_lower :
        return 'regular'
    else :
        return 'regular'

def load_fonts (font_list ,fonts_dict ):
    for name in font_list :
        p =ASSETS_DIR /name
        if not p .exists ():
            backup =p .with_suffix ('.bak')
            if backup .exists ():
                shutil .copy (backup ,p )
            else :
                continue
        try :
            if 'Emoji'in name or name .endswith ('.otf'):
                continue
            font =ImageFont .truetype (str (p ),52 ,index =0 )
            style =classify_font_style (name )
            fonts_dict [style ].append (font )
            if not p .with_suffix ('.bak').exists ():
                shutil .copy (p ,p .with_suffix ('.bak'))
        except Exception as e :
            if name .endswith ('.ttc'):
                for idx in range (8 ):
                    try :
                        font =ImageFont .truetype (str (p ),52 ,index =idx )
                        style =classify_font_style (name )
                        fonts_dict [style ].append (font )
                        break
                    except :
                        continue
load_fonts (ARM_FONTS ,FONTS )
load_fonts (GLOBAL_FONTS ,FONTS )
for style in FONTS :
    if not FONTS [style ]:
        FONTS [style ].append (ImageFont .load_default (size =52 ))

def load_font_with_fallback (size ,style ='regular'):
    target_fonts =ALL_FONTS
    for name in target_fonts :
        p =ASSETS_DIR /name
        if p .exists ():
            try :
                if 'Emoji'in name or name .endswith ('.otf'):
                    continue
                file_style =classify_font_style (name )
                if file_style !=style :
                    continue
                return ImageFont .truetype (str (p ),size ,index =0 )
            except :
                if name .endswith ('.ttc'):
                    for idx in range (8 ):
                        try :
                            return ImageFont .truetype (str (p ),size ,index =idx )
                        except :
                            continue
                continue
    return ImageFont .load_default (size =size )
W ,H =(1320 ,760 )
BG_BLUR =22
CIRCLE_SIZE =560
WHITE =(255 ,255 ,255 ,255 )
SHADOW =(0 ,0 ,0 ,100 )
TEXT_SHADOW =(0 ,0 ,0 ,130 )
STROKE_COLOR =(0 ,0 ,0 ,255 )

def resize_fit (img ,w ,h ):
    r =min (w /img .width ,h /img .height )
    return img .resize ((int (img .width *r ),int (img .height *r )),Image .LANCZOS )

def dominant_color (img ):
    img =img .resize ((60 ,60 ))
    pixels =list (img .getdata ())
    colors ={}
    for p in pixels :
        if len (p )==3 :
            p =(*p ,255 )
        if p [3 ]<150 :
            continue
        colors [p ]=colors .get (p ,0 )+1
    return max (colors ,key =colors .get )[:3 ]if colors else (40 ,40 ,80 )

def gradient_bg (draw ,w ,h ,color ):
    r ,g ,b =color
    for y in range (h ):
        factor =y /h
        col =(int (r *(1 -0.25 *factor )),int (g *(1 -0.25 *factor )),int (b *(1 -0.25 *factor )))
        draw .line ([(0 ,y ),(w ,y )],fill =(*col ,255 ))

def get_script (char ):
    try :
        codepoint =ord (char )
        if 0 <=codepoint <=127 or 160 <=codepoint <=383 or 384 <=codepoint <=591 or (7680 <=codepoint <=7935 )or (11360 <=codepoint <=11391 )or (42784 <=codepoint <=43007 ):
            return 'Zyyy'
        if 1328 <=codepoint <=1423 :
            return 'Armn'
        if 1536 <=codepoint <=1791 or 1872 <=codepoint <=1919 or 2208 <=codepoint <=2303 or (64336 <=codepoint <=65023 )or (65136 <=codepoint <=65279 ):
            return 'Arab'
        if 2432 <=codepoint <=2559 :
            return 'Beng'
        if 2304 <=codepoint <=2431 :
            return 'Deva'
        if 4608 <=codepoint <=5023 or 11648 <=codepoint <=11743 or 43776 <=codepoint <=43823 :
            return 'Ethi'
        if 4256 <=codepoint <=4351 or 11520 <=codepoint <=11567 :
            return 'Geor'
        if 880 <=codepoint <=1023 or 7936 <=codepoint <=8191 :
            return 'Grek'
        if 2688 <=codepoint <=2815 :
            return 'Gujr'
        if 1424 <=codepoint <=1535 or 64285 <=codepoint <=64335 :
            return 'Hebr'
        if 3200 <=codepoint <=3327 :
            return 'Knda'
        if 6016 <=codepoint <=6143 :
            return 'Khmr'
        if 3712 <=codepoint <=3839 :
            return 'Laoo'
        if 3328 <=codepoint <=3455 :
            return 'Mlym'
        if 4096 <=codepoint <=4255 :
            return 'Mymr'
        if 2816 <=codepoint <=2943 :
            return 'Orya'
        if 3456 <=codepoint <=3583 :
            return 'Sinh'
        if 2944 <=codepoint <=3071 :
            return 'Taml'
        if 3072 <=codepoint <=3199 :
            return 'Telu'
        if 1920 <=codepoint <=1983 :
            return 'Thaa'
        if 3584 <=codepoint <=3711 :
            return 'Thai'
        if 3840 <=codepoint <=4095 :
            return 'Tibt'
        if 1024 <=codepoint <=1279 or 1280 <=codepoint <=1327 or 11744 <=codepoint <=11775 or (42560 <=codepoint <=42655 )or (7296 <=codepoint <=7304 ):
            return 'Cyrl'
        return 'Zyyy'
    except Exception :
        return 'Zyyy'

def has_glyph (font ,char ):
    mask =font .getmask (char )
    bbox =mask .getbbox ()
    return bbox is not None and bbox [2 ]>0 and (bbox [3 ]>0 )

def draw_text_with_shadow_multi (draw ,xy ,text ,style ='regular',fill =WHITE ,stroke =4 ):
    fonts =FONTS .get (style ,FONTS ['regular'])
    all_fonts = fonts + [ImageFont.truetype(str(ASSETS_DIR / name), 52, index=0) for name in ALL_FONTS if (ASSETS_DIR / name).exists()]
    x ,y =xy
    for dx in [-stroke ,0 ,stroke ]:
        for dy in [-stroke ,0 ,stroke ]:
            if dx or dy :
                cx =x +dx
                for char in text :
                    font = next((f for f in all_fonts if has_glyph(f, char)), fonts[-1])
                    draw.text((cx, y+dy), char, fill=STROKE_COLOR, font=font)
                    cx += font.getlength(char)
    cx =x +2
    for char in text :
        font = next((f for f in all_fonts if has_glyph(f, char)), fonts[-1])
        draw.text((cx, y+2), char, fill=TEXT_SHADOW, font=font)
        cx += font.getlength(char)
    cx =x
    for char in text :
        font = next((f for f in all_fonts if has_glyph(f, char)), fonts[-1])
        draw.text((cx, y), char, fill=fill, font=font)
        cx += font.getlength(char)

def draw_text_shadow (draw ,xy ,text ,font ,fill =WHITE ,stroke =2 ):
    x ,y =xy
    for dx in [-stroke ,0 ,stroke ]:
        for dy in [-stroke ,0 ,stroke ]:
            if dx or dy :
                draw .text ((x +dx ,y +dy ),text ,fill =STROKE_COLOR ,font =font )
    draw .text ((x +1 ,y +1 ),text ,fill =TEXT_SHADOW ,font =font )
    draw .text ((x ,y ),text ,fill =fill ,font =font )

def split_text_multi (text ,style ='regular',max_w =0 ,max_lines =4 ):
    if not text or max_w <=0 :
        return [text ]if text else []
    fonts =FONTS .get (style ,FONTS ['regular'])
    words =text .split ()
    lines =[]
    cur_line =[]
    cur_width =0
    for word in words :
        space_width =fonts [0 ].getlength (' ')if cur_line else 0
        word_width =get_text_width_multi (word ,style )
        next_width =cur_width +space_width +word_width
        if next_width >max_w and cur_line :
            lines .append (' '.join (cur_line ))
            cur_line =[word ]
            cur_width =word_width
            if len (lines )>=max_lines :
                break
        else :
            cur_line .append (word )
            cur_width =next_width
    if cur_line :
        if len (lines )<max_lines :
            lines .append (' '.join (cur_line ))
        else :
            last_line =' '.join (cur_line )
            while get_text_width_multi (last_line ,style )>max_w and last_line :
                last_words =last_line .split ()
                if len (last_words )>1 :
                    last_line =' '.join (last_words [:-1 ])
                else :
                    last_line =last_words [0 ][:len (last_words [0 ])-1 ]
            lines [-1 ]=last_line
    return lines [:max_lines ]

def get_text_width_multi (text ,style ='regular'):
    fonts =FONTS .get (style ,FONTS ['regular'])
    all_fonts = fonts + [ImageFont.truetype(str(ASSETS_DIR / name), 52, index=0) for name in ALL_FONTS if (ASSETS_DIR / name).exists()]
    width = 0
    for char in text:
        font = next((f for f in all_fonts if has_glyph(f, char)), fonts[-1])
        width += font.getlength(char)
    return width
import asyncio

def format_views_count (views :int )->str :
    if views >=1000000000 :
        return f'{views /1000000000 :.1f}B'
    elif views >=1000000 :
        return f'{views /1000000 :.1f}M'
    elif views >=1000 :
        return f'{views /1000 :.1f}K'
    else :
        return str (views )

async def get_thumb (videoid :str ,queue_pos :int =1 ,title_style :str ='bold'):
    # Удалено: локальный кеш и CACHE_DIR. Всё хранится только в MongoDB.

    # song_data will be injected via Thumbnail.generate
    song_data = getattr(get_thumb, "_song_data", None)

    # Try to get thumbnail URL from yt-dlp (no mongo_cache, no logger)
    thumb_url = None
    title = getattr(song_data, "title", None) if song_data else None
    view_count = getattr(song_data, "views", None) if song_data else None
    channel_name = getattr(song_data, "channel", None) if song_data else None
    duration = getattr(song_data, "duration", None) if song_data else None
    # Если song_data содержит thumbnail, используем только его
    if song_data and getattr(song_data, "thumbnail", None):
        thumb_url = song_data.thumbnail
        thumb_candidates = [thumb_url]
    else:
        try:
            if title is None or view_count is None:
                try:
                    with YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl:
                        info = ydl.extract_info(f'https://youtu.be/{videoid}', download=False)
                except Exception as ydl_error:
                    thumb_url = f'https://i.ytimg.com/vi/{videoid}/maxresdefault.jpg'
                else:
                    if info:
                        if title is None:
                            title = info.get('title', 'Unknown Song')
                        if view_count is None:
                            view_count = info.get('view_count', 0)
                        thumb_url = info.get('thumbnail', '')
                        if channel_name is None:
                            channel_name = info.get('uploader', 'Unknown')
                        if duration is None:
                            duration = info.get('duration_string', '0:00')
                    else:
                        thumb_url = f'https://i.ytimg.com/vi/{videoid}/maxresdefault.jpg'
            else:
                thumb_url = f'https://i.ytimg.com/vi/{videoid}/maxresdefault.jpg'
        except Exception:
            thumb_url = f'https://i.ytimg.com/vi/{videoid}/maxresdefault.jpg'

        # Fallback chain for YouTube thumbnails
        thumb_candidates = []
        if thumb_url:
            thumb_candidates.append(thumb_url)
        # Always try these as fallback
        thumb_candidates.extend([
            f'https://i.ytimg.com/vi/{videoid}/maxresdefault.jpg',
            f'https://i.ytimg.com/vi/{videoid}/hqdefault.jpg',
            f'https://i.ytimg.com/vi/{videoid}/sddefault.jpg',
            f'https://i.ytimg.com/vi/{videoid}/mqdefault.jpg',
            f'https://i.ytimg.com/vi/{videoid}/default.jpg',
        ])

    img_data = None
    used_url = None
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as s:
        for url in thumb_candidates:
            try:
                async with s.get(url) as r:
                    if r.status == 200:
                        img_data = await r.read()
                        used_url = url
                        break
                    else:
                        print(f'[Thumb Error] Failed to download thumbnail from {url}: HTTP {r.status}')
            except Exception as e:
                print(f'[Thumb Error] Exception for {url}: {e}')
    if not img_data:
        return config.DEFAULT_THUMB

    try:
        if title is None:
            title = 'Unknown Song'
        if view_count is None:
            view_count = 0
        if channel_name is None:
            channel_name = 'Unknown'
        if duration is None:
            duration = '0:00'

        from io import BytesIO
        tmp = BytesIO(img_data)
        art = Image.open(tmp).convert('RGBA')
        dom = dominant_color(art)
        bg_art = resize_fit(art, W, H).filter(ImageFilter.GaussianBlur(BG_BLUR))
        bg_art = ImageEnhance.Brightness(bg_art).enhance(0.88)
        bg_art = ImageEnhance.Contrast(bg_art).enhance(1.05)
        canvas = Image.new('RGBA', (W, H), (0, 0, 0, 0))
        draw = ImageDraw.Draw(canvas)
        gradient_bg(draw, W, H, dom)
        canvas.paste(bg_art.convert('RGB'), (0, 0), bg_art.split()[3] if bg_art.mode == 'RGBA' else None)
        x, y = (80, (H - CIRCLE_SIZE) // 2)
        r = max(CIRCLE_SIZE / art.width, CIRCLE_SIZE / art.height)
        circle_w = int(art.width * r)
        circle_h = int(art.height * r)
        circle = art.resize((circle_w, circle_h), Image.LANCZOS)
        mask = Image.new('L', (CIRCLE_SIZE, CIRCLE_SIZE), 0)
        mdraw = ImageDraw.Draw(mask)
        mdraw.ellipse((0, 0, CIRCLE_SIZE, CIRCLE_SIZE), fill=255)
        circle.putalpha(mask.resize((circle_w, circle_h), Image.LANCZOS))
        circle_canvas = Image.new('RGBA', (CIRCLE_SIZE, CIRCLE_SIZE), (0, 0, 0, 0))
        circle_canvas.paste(circle, ((CIRCLE_SIZE - circle_w) // 2, (CIRCLE_SIZE - circle_h) // 2))
        circle_canvas.putalpha(mask)
        shadow = Image.new('RGBA', (CIRCLE_SIZE + 100, CIRCLE_SIZE + 100), (0, 0, 0, 0))
        sdraw = ImageDraw.Draw(shadow)
        sdraw.ellipse((50, 50, CIRCLE_SIZE + 50, CIRCLE_SIZE + 50), fill=SHADOW)
        shadow = shadow.filter(ImageFilter.GaussianBlur(35))
        canvas.paste(shadow, (x - 50, y - 50), shadow)
        canvas.paste(circle_canvas, (x, y), circle_canvas)
        info_x = x + CIRCLE_SIZE + 80
        max_w = W - info_x - 70

        clean_title = remove_emojis(title)
        title_lines = split_text_multi(clean_title, style=title_style, max_w=max_w, max_lines=4)
        line_height = 58
        total_h = len(title_lines) * line_height
        t_y = y + (CIRCLE_SIZE - total_h) // 2 + 8
        for i, line in enumerate(title_lines):
            line_w = get_text_width_multi(line, style=title_style)
            tx = info_x + (max_w - line_w) // 2
            draw_text_with_shadow_multi(draw, (tx, t_y + i * line_height), line, style=title_style, stroke=4)
        try:
            views = int(view_count or 0)
        except:
            views = 0
        views_text = f'{format_views_count(views)} views'
        views_font = load_font_with_fallback(32, style='regular')
        v_w = views_font.getlength(views_text)
        v_x = info_x + (max_w - v_w) // 2
        v_y = t_y + total_h + 8
        draw_text_shadow(draw, (v_x, v_y), views_text, views_font, fill=(230, 230, 230, 230), stroke=2)
        water = 'Moon Music'
        water_font = load_font_with_fallback(28, style='italic')
        w_w = water_font.getlength(water)
        wx = W - w_w - 30
        wy = 10
        draw_text_shadow(draw, (wx, wy), water, water_font, fill=(210, 210, 210, 190), stroke=2)
        canvas = canvas.convert('RGB')
        out_bytes = BytesIO()
        canvas.save(out_bytes, 'JPEG', quality=99, optimize=True, subsampling=0)
        out_bytes.seek(0)
        # Сохраняем в mongo_cache с TTL
        if hasattr(get_thumb, 'mongo_cache') and get_thumb.mongo_cache is not None:
            await get_thumb.mongo_cache.save_file(f"thumb:{videoid}", out_bytes.getvalue(), {"video_id": videoid, "createdAt": __import__('datetime').datetime.utcnow()})
        return out_bytes.getvalue()
    except Exception as e:
        print(f'[Thumb Error] {e}')
        traceback.print_exc()
        return config.DEFAULT_THUMB

class Thumbnail :

    async def generate(self, song, size=None):
        # Пробрасываем все поля song в song_data
        class SongData:
            pass
        s = SongData()
        for attr in ["id", "title", "views", "channel", "uploader", "duration", "thumbnail"]:
            setattr(s, attr, getattr(song, attr, None))
        get_thumb._song_data = s
        try:
            return await get_thumb(s.id)
        finally:
            get_thumb._song_data = None
