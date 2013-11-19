;;; Directory Local Variables
;;; See Info node `(emacs) Directory Variables' for more information.

((nil
  (eval . (setenv "CLASSPATH" (concat ".:" (let (val) (dolist (elt (cons (file-truename "iongun.jar") (directory-files (concat (let ((l (dir-locals-find-file (or (buffer-file-name) default-directory)))) (if (listp l) (car l) l)) "lib") t ".*\.jar")) (substring val 1)) (setq val (concat val ":" elt)))))))
  (compile-command . (concat "make -k -C " (let ((l (dir-locals-find-file (or (buffer-file-name) default-directory)))) (if (listp l) (car l) l)))))
 (java-mode
  (eval . (setq semanticdb-javap-classpath (directory-files (concat (let ((l (dir-locals-find-file (or (buffer-file-name) default-directory)))) (if (listp l) (car l) l)) "lib") t ".*\.jar")))))

