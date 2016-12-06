from __future__ import unicode_literals
import copy_reg
import types
import pickle
import urllib2
import urllib
import time
import os
import gc
import threading
import glob

import youtube_dl

def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

copy_reg.pickle(types.MethodType, _pickle_method)

class YTCrawl(object):
    dkey = ''
    nthread = 10
    odir = ''
    region = 'US'
    oformat = ''
    bquality = 'low'
    maxsize = '100M'
    
    txtres = {
        'video' : {},
        'channel' : {},
        'searchchannel' : {},
        'searchvideo' : {}
    }

    def __init__(self, dkey, odir='./data/', nthread=10, oformat='pickle', region='US', bquality='low', flushlog=False):
        self.odir = odir
        self.dkey = dkey
        self.nthread = nthread;
        self.oformat = oformat
        self.region = 'US'
        self.bquality = bquality

        self.mdelay = threading.Lock()
        self.sdelay = .05    # in seconds

        self.errfile = self.odir + '/' + 'log.error'
        self.donefile = self.odir + '/' + 'log.done'
        self.flushlog = flushlog

    # crawl functions
    def crawl(self, t, kl):
        """
        t: data type ('video', 'channel', 'search', 'binary')
        kl: key list (videoID, channelID, or search keywords)
        """

        old = self.loginit()
        if old != None:
            kl = list(set(kl)-old) # remove old ones
        
        if t == 'channel':
            self.nrmap(self.t_channel, kl)
            return
        if t == 'video':
            self.nrmap(self.t_video, kl)
            return
        if t == 'search':
            self.nrmap(self.t_searchVideo, kl)
            return
        if t == 'binary':
            self.nrmap(self.t_binary, kl)


    # no return map
    def nrmap(self, func, kwl):
        threads = []
        nEach = len(kwl)/self.nthread
        nt = self.nthread
        if nEach == 0:
            nt = len(kwl)
        for i in range(0, nt-1):
            threads.append(threading.Thread(target=func, args=(kwl[i*nEach : (i+1)*nEach], )))
        threads.append(threading.Thread(target=func, args=(kwl[i*nEach : ], )))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.flush()

    def gdelay(self):
        "global delay"
        self.mdelay.acquire()
        time.sleep(self.sdelay)
        self.mdelay.release()      
        
    # thread functions, called by crawling functions
    def t_searchVideo(self, kwl):
        for kw in kwl:
            self.s_searchVideo(kw)

    def t_binary(self, kwl):
        for kw in kwl:
            self.s_binary(kw)

    def t_video(self, kwl):
        for kw in kwl:
            self.s_video(kw)

    def s_searchVideo(self, kw):
        "to donwload a single search result"
        try:
            url = ('https://www.googleapis.com/youtube/v3/search?part=snippet' + \
                   '&' + urllib.urlencode({'q':kw}) + \
                   '&maxResults=50' + \
                   '&regionCode=' + self.region + \
                   '&type=video' + \
                   '&order=videoCount' + \
                   '&key=' + self.dkey).replace( ' ', '\%20')
        except Exception, e:
            print e, kw
            self.logerr(kw + ',' + str(e))
            return
        
        self.gdelay()

        try:
            txt = urllib2.urlopen(url).read()
        except Exception, e:
            print e
            self.logerr(kw + ',' + str(e))
            return
        
        self.save_txt(kw, txt, 'searchvideo')
        return


    def s_video(self, vId):
        "to donwload a single video meta data"
        txt = urllib2.urlopen( \
            'https://www.googleapis.com/youtube/v3/videos?id=' + vId + \
            '&key=' + self.dkey + \
            '&regionCode=' + self.region + \
            '&part=snippet,contentDetails,player,recordingDetails,statistics,status,topicDetails').read()
        self.save_txt(vId, txt, 'video')
        return


    def s_channel(self, cId):
        "to download a single channel meta data"
        raise('Not implemented yet')
        return


    def s_binary(self, vId):
        "to download a single video content"
        d = self.odir + '/' + vId[-1] + '/' + vId[-2] + '/'
        self.mkdir(d)
        ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessors': [{}],
            'logger': (self.YtdlLogger(self.errfile, self.flushlog)),
            'progress_hooks': [self.ytdl_hook],
            'outtmpl': d + r'%(id)s.%(ext)s',
        }
        
        ydl = youtube_dl.YoutubeDL(ydl_opts)
        ydl.download([vId])
        return

    class YtdlLogger(object):
        def __init__(self, logfile, flush=False):
            self.logfile = logfile
            self.flush = flush
            
        def debug(self, msg):
            pass
        def warning(self, msg):
            self.logfile.write('[warning] ' + msg + '\n')
            if self.flush:
                self.logfile.flush()
        def error(self, msg):
            self.logfile.write('[error] ' + msg + '\n')
            if self.flush:
                self.logfile.flush()

    def ytdl_hook(self, m):
        if d['status'] == 'finished':
            vId = d['filename'].split('.')[-2][-11:]
            self.logdone(vId)
            

    # output functions
    def save_txt(self, Id, txt, dtype):
        if self.oformat == 'pickle':
            if dtype[0:6] == 'search':
                self.txtres[dtype].setdefault(Id,[])
                self.txtres[dtype][Id].append(txt)
            else:
                self.txtres[dtype][Id] = txt
        elif self.oformat == 'hashfolder':
            ## use -1, -2 because for channelID, the starting chars are always 'UC'
            d = self.odir + '/' + dtype + '/' + Id[-1] + '/' + Id[-2] + '/'
            self.mkdir(d)
            open(d + Id, 'w').write(txt)
            self.logdone(Id)
        return


    def flush(self):
        "write the txt result to disk"
        if self.oformat == 'pickle':
            for k, v in self.txtres.iteritems():
                if len(v) != 0:
                    meta = 'region' + self.region + '_' + \
                           'time' + time.strftime('%Y-%m-%d-%H-%M-%S')
                    d = self.odir + '/'
                    self.mkdir(d)
                    pickle.dump(v, open(d + k + '_' +  meta + '.pickle', 'w'))
                    for Id in v.iterkeys():
                        self.logdone(Id)
            kl = self.txtres.keys()
            self.txtres = {}
            gc.collect()
            for k in kl:
                self.txtres[k] = {};
                    
        return

    # log functions
    ## log.error
    ## log.done
    def loginit(self):
        """Check if this exist a crawling database and create file.
        If old files exist, return the ids that has been done.
        else return None;
        """

        if os.path.exists(self.errfile) and \
           os.path.exists(self.donefile):
            self.errfileobj = open(self.errfile, 'a+r')
            self.donefileobj = open(self.donefile, 'a+r')

            ret = set()
            for l in open(self.donefile):
                ret.add(l.strip('\n\t '))
            return ret
        else:
            self.errfileobj = open(self.errfile, 'w+r')
            self.donefileobj = open(self.donefile, 'w+r')
            return None

    def logerr(self, s, flush=False):
        self.errfileobj.write(s + '\n')
        if flush:
            self.errfileobj.flush()

    def logdone(self, s, flush=False):
        self.donefileobj.write(s + '\n')
        if flush:
            self.donefileobj.flush()

    # helper functions
    def mkdir(self, d):
        if not os.path.exists(d):
            os.makedirs(d)


class Utility(object):

    nthread = 16
    threadbatch = 50
    
    def __init__(self, nthread):
        self.nthread = nthread
    
    def video2mp3(self, indir, outdir):
        "recursively clone the input folder"
        if not self.which('ffmpeg'):
            raise 'Error: cannot find ffmpeg in PATH'

        batch = self.nthread*self.threadbatch

        arglist = []
        N = 0
        for rp, f in self.relwalkfile(indir):
            if N < batch:
                fn = '.'.join(f.split('.')[0:-1])
                outfile = outdir+'/'+rp+'/'+fn+'.mp3'
                if os.path.exists(outfile):
                    continue
                arglist.append((indir+'/'+rp+'/'+f, outfile))
                N += 1
            else:
                self.nrmap(self.t_video2mp3, arglist)
                arglist = []
                N = 0
        if arglist != None:
            self.nrmap(self.t_video2mp3, arglist)
                
    def t_video2mp3(self, arg):
        for infile, outfile in arg:
            self.mkdir(os.path.dirname(outfile))
            os.system('ffmpeg -loglevel panic -n -i ' + infile + ' -vn ' + outfile)


    # support for parallel
    def nrmap(self, func, kwl):
        ## no return map
        threads = []
        nEach = len(kwl)/self.nthread
        nt = self.nthread
        if nEach == 0:
            nt = len(kwl)
        for i in range(0, nt-1):
            threads.append(threading.Thread(target=func, args=(kwl[i*nEach : (i+1)*nEach], )))
        threads.append(threading.Thread(target=func, args=(kwl[(nt-1)*nEach : ], )))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    
    # utility functions
    def getdir(self, p):
        topdir = []
        tmp = glob.glob(p + '/*')
        for i in tmp:
            if os.path.isdir(i):
                topdir.append(i)
                
        ret = []
        for d in topdir:
            tmp = d.split(os.sep)
            ret.append(filter(lambda x: x != '', tmp)[-1])
        
        return ret
    
    # helper functions
    def mkdir(self, d):
        if not os.path.exists(d):
            os.makedirs(d)
            
    def which(self, program):
        import os
        def is_exe(fpath):
            return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

        fpath, fname = os.path.split(program)
        if fpath:
            if is_exe(program):
                return program
        else:
            for path in os.environ["PATH"].split(os.pathsep):
                path = path.strip('"')
                exe_file = os.path.join(path, program)
                if is_exe(exe_file):
                    return exe_file
        return None

    def relwalkfile(self, folder):
        "return relative path and file name"
        for root, dirs, files in os.walk(folder):
            for f in files:
                yield (os.path.relpath(root,folder), f)
    
    def printinput(self, i):
        "for debug"
        print i
