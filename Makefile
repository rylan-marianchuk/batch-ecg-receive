all:
	nvcc -Xcompiler -fPIC -shared -o wvfm_features.so ./src/wvfm_features.cu
	mv wvfm_features.so ./src/
clean:
	rm ./src/wvfm_features.so
	rm ./database/*.db
	rm /home/rylan/ecgh5/*
