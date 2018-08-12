;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")

((nil
  (compile-command . "cd `git rev-parse --show-toplevel` && mvn -B install"))
 (java-mode
  (eval setq semanticdb-javap-classpath
	(directory-files
	 (concat
	  (let
	      ((l
		(dir-locals-find-file
		 (or
		  (buffer-file-name)
		  default-directory))))
	    (if
		(listp l)
		(car l)
	      l))
	  "lib")
	 t ".*.jar"))))


