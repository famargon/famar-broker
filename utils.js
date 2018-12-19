const fs = require('fs');

const createDirectory = function(path){
    return new Promise((resolve,reject)=>{
        fs.mkdir(path,(mkdirErr)=>{
            if(mkdirErr){
                reject(mkdirErr);
                return;
            }
            fs.open(path, 'r', (err, fd) => {
                if(err){
                    reject(err);
                }else{
                    resolve();
                }
            });
        });
    });
}

const checkDirectory = function(path){
    return new Promise((resolve,reject)=>{
        fs.open(path, 'r', (err, fd) => {
            if(err){
                if (err.code === 'ENOENT') {
                    createDirectory(path)
                    .then(()=>resolve())
                    .catch(dirErr=>{
                        reject(dirErr);
                    });
                }else{
                    reject(err);
                }
            }else{
                resolve();
            }
        });
    });
}

module.exports.checkDirectory = checkDirectory;