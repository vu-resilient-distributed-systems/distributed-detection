# implement darknet detector
from __future__ import division
import torch
import numpy as np
import cv2 
import matplotlib.pyplot as plt
import time
import random
import _pickle as pkl
from torch.autograd import Variable

try:
    from bbox import bbox_iou
    from darknet import Darknet
    from util import load_classes, write_results
except:
    from simple_yolo.bbox import bbox_iou
    from simple_yolo.darknet import Darknet
    from simple_yolo.util import load_classes, write_results

class Darknet_Detector():
    def __init__(self, id_num, cfg_file,wt_file,class_file,pallete_file, nms_threshold = .3 , conf = 0.7, resolution=1024, num_classes=80, nms_classwise= True):
        #Set up the neural network
        print("Loading network.....")
        self.model = Darknet(cfg_file)
        self.model.load_weights(wt_file)
        print("Network successfully loaded")
        
        self.nms = nms_threshold
        self.conf = conf
        self.nms_classwise = nms_classwise
        self.resolution = resolution # sets size of max dimension
        
        if id_num == 0:
            self.CUDA = True
            torch.cuda.set_device(0)
            torch.cuda.empty_cache()
            
        elif id_num == 1:
            self.CUDA = True
            torch.cuda.set_device(1)
            torch.cuda.empty_cache()
        else:
            self.CUDA = False
            
        self.colors = pkl.load(open(pallete_file, "rb"))
        self.num_classes = num_classes
        self.classes = load_classes(class_file) 
    

        
        self.model.net_info["height"] = self.resolution
        inp_dim = int(self.model.net_info["height"])
        assert inp_dim % 32 == 0 
        assert inp_dim > 32
    
        #If there's a GPU availible, put the model on GPU
        if self.CUDA:
            self.model.cuda()
        
        
        #Set the model in evaluation mode
        self.model.eval()
        
        
    def prep_image(self,img,inp_dim):
        """
        Prepare image for inputting to the neural network. 
        Returns a Variable 
        """
        orig_im = img
        dim = orig_im.shape[1], orig_im.shape[0]
        img = cv2.resize(orig_im, (inp_dim, inp_dim))
        img_ = img[:,:,::-1].transpose((2,0,1)).copy()
        img_ = torch.from_numpy(img_).float().div(255.0).unsqueeze(0)
        return img_, orig_im, dim

#def write(x, img):
#    c1 = tuple(x[1:3].int())
#    c2 = tuple(x[3:5].int())
#    cls = int(x[-1])
#    label = "{0}".format(classes[cls])
#    color = random.choice(colors)
#    cv2.rectangle(img, c1, c2,color, 1)
#    t_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_PLAIN, 1 , 1)[0]
#    c2 = c1[0] + t_size[0] + 3, c1[1] + t_size[1] + 4
#    cv2.rectangle(img, c1, c2,color, -1)
#    cv2.putText(img, label, (c1[0], c1[1] + t_size[1] + 4), cv2.FONT_HERSHEY_PLAIN, 1, [225,255,255], 1);
#    return img
#        orig_im = img
#        dim = orig_im.shape[1], orig_im.shape[0]
#        img = cv2.resize(orig_im, (inp_dim, inp_dim))
#        img_ = img[:,:,::-1].transpose((2,0,1)).copy()
#        img_ = torch.from_numpy(img_).float().div(255.0).unsqueeze(0)
#        return img_, orig_im, dim
    

    def write(self,x, img):
        c1 = tuple(x[1:3].int())
        c2 = tuple(x[3:5].int())
        cls = int(x[-1])
        label = "{0}".format(self.classes[cls])
        color = random.choice(self.colors)
        cv2.rectangle(img, c1, c2,color, 1)
        t_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_PLAIN, 1 , 1)[0]
        c2 = c1[0] + t_size[0] + 3, c1[1] + t_size[1] + 4
        cv2.rectangle(img, c1, c2,color, -1)
        cv2.putText(img, label, (c1[0], c1[1] + t_size[1] + 4), cv2.FONT_HERSHEY_PLAIN, 1, [225,255,255], 1);
        return img    
        
    
    
    def detect(self,image, show = False,verbose = False,save_file = None):
        start = time.time()
#        
        try: # image is already loaded
            img, orig_im, dim = self.prep_image(image, self.resolution)
        except: # image is a file path
            image = cv2.imread(image)
            img, orig_im, dim = self.prep_image(image, self.resolution)
            
        im_dim = torch.FloatTensor(dim).repeat(1,2)                        
            
        if self.CUDA:
            im_dim = im_dim.cuda()
            img = img.cuda()
        
        
        output = self.model(Variable(img), self.CUDA)
        output = write_results(output, self.conf, self.num_classes, nms = True, nms_conf = self.nms)
        output[:,1:5] = torch.clamp(output[:,1:5], 0.0, float(self.resolution))/self.resolution
        
        im_dim = im_dim.repeat(output.size(0), 1)
        output[:,[1,3]] *= image.shape[1]
        output[:,[2,4]] *= image.shape[0]

                
        out = list(map(lambda x: self.write(x, orig_im), output))
        
        if verbose:
            print("FPS of the video is {:5.2f}".format( 1.0 / (time.time() - start)))
            
        if save_file != None:
            cv2.imwrite(save_file, orig_im)    
            
        if show:
            cv2.imshow("frame", orig_im)
            cv2.waitKey(0)


            
        
       
        return output, orig_im
        
        
if __name__ == "__main__":
     
    try:
        net2
    except:
        net2 = Darknet_Detector(0,'cfg/yolov3.cfg','yolov3.weights','data/coco.names','pallete')
    
    test_file = 'imgs/img3.jpg'
    out, im = net2.detect(test_file, show = True)
    cv2.imwrite("out1.jpg",im)
    cv2.destroyAllWindows()