# IAS3 Bulk Uploader

The IAS3 Bulk Uploader is a Perl script that automates uploading multiple files to [archive.org](http://archive.org) using the Internet Archive's S3-like API. It should run on Linux and Mac OS X with standard Perl libraries installed. It has not been tested for Windows. This tool is still in it's alpha stage and thus it may not handle various corner cases gracefully. Please [contact Internet Archive](mailto:info@archive.org?subject=[IAS3 Bulk Uploader]) with questions and design suggestions.

The intended users of this script are Internet Archive users interested in uploading batches of content alongside per-item metadata in an automated fashion. The user _should_ have a collection registered with the Internet Archive. Please [contact Internet Archive](mailto:info@archive.org?subject=[Collection Creation Request]) if you need a collection created.

## Bulk Uploading With ias3upload.pl

### Prepare your Metadata

+ Prepare a UTF-8 encoded CSV file with your metadata. Use the metadata.csv file included in this repository as a template.

+ There are five required metadata fields to be included in your CSV:

    + **item**: This is your Internet Archive Identifier. An identifier is composed of any unique combination of alphanumeric characters, underscore (_) and dash (-). While the official limit is 100 characters, it is strongly suggested that they be between 5 and 80 characters in length. Identifiers must be unique across the entirety of Internet Archive, not simply unique within a single collection. Once defined an identifier can not be changed. It will travel with the item or object and is involved in every manner of accessing or referring to the item.  

    + **creator:** An entity primarily responsible for creating the content contained in the item. If there are multiple creators each should have a separate column. For example if you had three creators, you should have three columns named in the following manner: `creator[0]`, `creator[1]`, `creator[2]`  

    + **file**: Put the filename of your file in this field. If you are uploading multiple files to a single item, simply add a new line for each file (Note you do not need to repopulate the metadata fields for each file, see the template `metadata.csv` for an example).  

    + **mediatype**: The primary type of media contained in the item. While an item can contain files of diverse mediatypes the value in this field defines the appearance and functionality of the item's detail page on Internet Archive. In particular, the mediatype of an item defines what sort of online viewer is available for the files contained in the item.

        The mediatype metadata field recognizes a limited set of values:

        **audio**: The majority of audio items should receive this mediatype value. Items for the Live Music Archive should instead use the etree value.  

        **etree**: Items which contain files for the [Live Music Archive](http://www.archive.org/details/etree) should have a mediatype value of etree. The Live Music Archive has very specific upload requirements. Please consult [the documentation](http://wiki.etree.org/index.php?page=SeedingGuidelines) for the Live Music Archive prior to creating items for it.  

        **image**: Items which predominantly consist of image files should receive a mediatype value of image. Currently these items will not available for browsing or online viewing in Internet Archive but they will require no additional changes when this mediatype receives additional support in the Archive.  
        
        **movies**: All videos (television, features, shorts, etc.) should receive a mediatype value of movies. These items will be displayed with an online video player.  

        **software**: Items with a mediatype of software are accessible to browse via Internet Archive's software collection. There is no online viewer for software but all files are available for download.   

        **texts**: Items with a mediatype of texts will appear with the online bookreader. Internet Archive will also attempt to OCR files in these items.  

        **web**: The web mediatype value is reserved for items which contain web archive WARC files.  

        If the mediatype value you set is not in the list above it will be saved but ignored by the system.   

        This field may be modified only by an administrator or the owner of the item.  

   + **collection**: A collection is a specialized item used for curation and aggregation of other items. Assigning an item to a collection defines where the item may be located by a user browsing Internet Archive. A collection must exist prior to assigning any items to it. You must have admin privileges for the collection in order to upload files to that collection. Currently collections can only be created by Internet Archive staff members. Please contact Internet Archive if you need a collection created.  

+ Add any other, non-required, metadata fields desired. 

    + **Recognized Metadata Fields**: There are several standard metadata fields recognized for Internet Archive items. Besides the fields mentioned above, you may want to include the following:

        **title**: The title for the item. This appears in the header of the item's detail page on Internet Archive. If a value is not specified for this field it will default to the identifier for the item.  
        
        **description**: A description of the item.

        **date**: The publication, production or other similar date of the content in this item. Please use an ISO 8601 compatible format for this date. For instance, these are all valid date formats: `YYYY`, `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`

        **subject**: Keyword(s) or phrase(s) that may be searched for to find your item. For each subject, create a new column. For example if you had three subjects, you should have three columns named in the following manner: `subject[0]`, `subject[1]`, `subject[2]` 

        **licenseurl**: A **URL** to the license which covers the works contained in the item. If you would like to use a Creative Commons license on your item, you may find the URL to various licenses [here](http://creativecommons.org/licenses/) (i.e. `http://creativecommons.org/licenses/by/3.0/`)  
         
        **rights**: The value of the rights metadata field should be a statement of the rights held in and over the item. 

        **publisher**: The publisher of the material available in the item. 

        **contributor**: The value of the contributor metadata field is information about the entity responsible for making contributions to the content of the item. This is often the library, organization or individual making the item available on Internet Archive. 
        
        **language**: The primary language of the material available in the item. While the value of the language metadata field can be any value, Internet Archive prefers they be [MARC21 Language Codes](http://www.loc.gov/marc/languages/language_code.html). 

        **credits**: If known, enter the participants in the production of the materials contained in the item in the credits metadata field.

        **stream_only**: For audio and movies mediatype it is possible to only allow the content to be streamed. While not recommended, if this is necessary it can be accomplished by adding stream_only as a collection value.   

    + **Custom Metadata Fields**: Internet Archive strives to be metadata agnostic, enabling users to define the metadata format which best suits the needs of their material. In addition to the standard metadata fields listed above you may also define as many custom metadata fields as you require. These metadata fields can be defined ad hoc at item creation or metadata editing time and do not have to be defined in advance. To include custom metadata, simply add a column with your desired field as the heading. Custom metadata fields should be alphanumeric and not contain any spaces. For mediatype texts items, the custom metadata will be displayed on the details page.   


+ Save your prepared CSV file as `metadata.csv` 



### Obtain your S3 Keys and prepare to upload. 

+ Create a directory and move the `ias3upload.pl` script, your CSV file and the media you intend to upload into it.

+ Get your S3 API keys by [clicking here](http://www.archive.org/account/s3.php). You must be logged into the Internet Archive to access your keys.

+ Open up the _Terminal_ application on your computer. Enter the following command, replacing `Y6oUrAcCEs4sK8ey` with your _access key_ and `youRSECRETKEYzZzZ` with your _secret key_, into the terminal window and hit return.

        export IAS3KEYS=Y6oUrAcCEs4sK8ey:youRSECRETKEYzZzZ

+ From the Terminal window, move into the directory you created (containing `metadata.csv` and the files you intend to upload). In Mac OS X, you can simply type `cd` into the Terminal folder followed by a space and dragging your folder into the Terminal window and hitting return.

+ Enter the following command and hit return (make sure you are in the same directory in which `ias3upload.pl` is located):

        chmod +x ias3upload.pl

+ Run ias3upload.pl by entering the following command into the Terminal window and hitting return:

        user$ ./ias3upload.pl

+ If you do not want your items to derive (i.e. if you're uploading compressed data files, or files you do not want the Internet Archive to convert into other formats), use the --no-derive option:
        
        user$ ./ias3upload.pl --no-derive

+ You may also use the -l option to point to your CSV file. This is helpful if your CSV file is named something other than metadata.csv or is located in another directory:
 
        user$ ./ias3upload.pl -l /Users/user/Desktop/music-metadata.csv

+ Your Terminal window should start printing lines and look something like the following:

        user$ ./ias3upload.pl
        Uploading /Users/user/Pictures/IMG_5243.JPG
        Sent 541598 bytes (100%)
        201 Created

        Uploading /Users/user/Pictures/IMG_5246.JPG
        Sent 506207 bytes (100%)
        201 Created

        Uploading /Users/user/Pictures/IMG_5248.JPG
        Sent 490214 bytes (100%)
        201 Created

+ When you see `201 Created`, your item has successfully made its way to a staging server at the Internet Archive. Your job is done, and you can rest assured that your item has been successfully archived. If you see any thing other than `201 Created`, there may have been issues with your upload.

+ After you receive the `201 Created` message, the Internet Archive will start to build your item. Before your item is created, a few automated tasks will have to run (archive.php, bup.php, derive.php). You can monitor these tasks by viewing [your catalog on archive.org](http://www.archive.org/catalog.php?&justme=1).

+ The URL for your items will be `http://www.archive.org/details/${IDENTIFIER}` (replacing `${IDENTIFIER}` with the Internet Archive Identifier of your item).

Please [contact Internet Archive](mailto:info@archive.org?subject=[Collection Creation Request) with any questions.

