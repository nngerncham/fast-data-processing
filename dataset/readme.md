# Downloading the datasets

Make sure that you have the [`kaggle` CLI tool](https://github.com/Kaggle/kaggle-api) set up for some of these.

- Enron email data set
  ```sh
  wget -P enron-emails/ https://www.cs.cmu.edu/\~enron/enron_mail_20150507.tar.gz
  tar -xf enron_mail_20210507.tar.gz -C enron-emails/
  ```
  ```
  ```

- Steam Reviews from 2021
  ```sh
  kaggle datasets download najzeko/steam-reviews-2021
  ```
