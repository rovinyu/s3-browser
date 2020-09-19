import boto3
from boto3 import client
import bs4
import os

def getFilesAndFolderOfBucket(strBucket,strPrefix):
    conn = client('s3')
    sesFolder = conn.list_objects(Bucket=strBucket, Prefix=strPrefix, Delimiter='/')
    vecFiles = []
    vecFolders = []
    if (sesFolder.get('CommonPrefixes') != None):
        for key in sesFolder.get('CommonPrefixes'):
            vecFolders.append(key['Prefix'])
    if (sesFolder.get('Contents')!=None):
        for key in sesFolder.get('Contents'):
            vecFiles.append(key['Key'])
    print("vecFiles: ", end = '')
    print(vecFiles)
    print("vecFolders: ", end = '')
    print(vecFolders)

    return (vecFiles,vecFolders)

def uploadIndexFile(strBucket,strPrefix,strIndexFile):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(strBucket)
    bucket.upload_file(strIndexFile, strPrefix + strIndexFile,
                       ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/html'})

def generateIndexFile(strBucket,strPrefix,strIndexFile,vecFiles,vecFolders,strTemplate):
    with open(strTemplate) as inf:
        txt = inf.read()
        soup = bs4.BeautifulSoup(txt, features="html5lib")

    tagKeysList = soup.find("ul", {"id": "listkeys"})

    tagKeysList.append(generateHeader(soup, strBucket, strPrefix))

    for strFolder in vecFolders:
        strFolderLast = strFolder.split('/')[-2]
        tagKeysList.append(generateElement(soup, True, strFolderLast, '/' + strFolder + 'index.html'))

    vecFiles.sort(reverse = True)
    for strFile in vecFiles:
        strFileLast = strFile.split('/')[-1]
        if strFileLast == 'index.html' or strFileLast == '':
            continue
        tagKeysList.append(generateElement(soup, False, strFileLast, '/' + strFile))

    with open(strIndexFile, "w") as outf:
        outf.write(str(soup))

def recPopulateIndexFiles(strBucket,strPrefix,strTemplate):
    (vecFiles, vecFolders) = getFilesAndFolderOfBucket(strBucket, strPrefix)
    generateIndexFile(strBucket, strPrefix, strIndexFile, vecFiles, vecFolders,strTemplate)
    uploadIndexFile(strBucket, strPrefix, strIndexFile)
    for strFolder in vecFolders:
        recPopulateIndexFiles(strBucket, strFolder,strTemplate)

def generateElement(soup,flagIsFolder,strText,strURL):
    tagLi = soup.new_tag("li", **{'class': 'collection-item'})
    tagDiv = soup.new_tag("div", **{'class': 'valign-wrapper'})
    tagI = soup.new_tag("i", **{'class': 'material-icons iconitem'})
    if (flagIsFolder):
        tagI.string = 'folder_open'
    else:
        tagI.string = 'insert_drive_file'
    tagA = soup.new_tag("a", href=strURL)
    tagA.string = strText
    tagDiv.append(tagI)
    tagDiv.append(tagA)
    tagLi.append(tagDiv)
    return tagLi

def generateHeader(soup,strBucket,strPrefix):
    tagHeader = soup.new_tag("li", **{'class': 'collection-header'})
    tagH = soup.new_tag("h4")
    if strPrefix:
        tagH.string = strPrefix.strip('/')
    else:
        tagH.string = u'欢迎来到Rovin的小站'
    tagHeader.append(tagH)
    print(tagHeader)
    return tagHeader

def uploadFundRankFile(strBucket,strPrefix,strIndexFile):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(strBucket)
    bucket.upload_file(strIndexFile, strPrefix + strIndexFile,
                       ExtraArgs={'ACL': 'public-read'})

def uploadFundRankfiles(localPath, strBucket, strPrefix):
    currDir = os.getcwd()
    os.chdir(localPath)
    fileList = os.listdir(localPath)
    (vecFiles, vecFolders) = getFilesAndFolderOfBucket(strBucket, strPrefix)
    for file in fileList:
        if strPrefix+file not in vecFiles:
            uploadFundRankFile(strBucket, strPrefix, file)
    os.chdir(currDir)


if __name__ == '__main__':
    strBucket = 'fundrank'
    strPrefix = ''
    strIndexFile = 'index.html'
    strTemplate = 'index_template.html'
    localPath = u'/Users/rovin/Documents/基金数据'

    uploadFundRankfiles(localPath, strBucket, '基金排名/')

    recPopulateIndexFiles(strBucket, strPrefix, strTemplate)
