#!/usr/bin/perl

use strict;
use warnings;
use v5.10;
use utf8;

our $VERSION = 0.6;

use Getopt::Long;
use Pod::Usage;

process();

sub process {
  my $file = undef;
  my $keyword = undef;
  my $sites_limit = 50;
  my $pps_limit = 200;
  my $yandex_key = undef;
  my $yandex_user = undef;
  my $version = undef;

  GetOptions(
    'file|f=s' => \$file,
    'keyword|k=s' => \$keyword,
    'sites=i' => \$sites_limit,
    'pages=i' => \$pps_limit,
    'yandex_key|y=s' => \$yandex_key,
    'yandex_user|u=s' => \$yandex_user,
    'version|v' => \$version,
  );

  if ( $version ) {
    say $VERSION;
    return;
  }

  if ( !( $file && $keyword && $yandex_key && $yandex_user ) ) {
    pod2usage( 1 );
    return;
  }

  my $anch = Anchors->new();
  $anch->process( $keyword, $file, $sites_limit, $pps_limit, $yandex_key, $yandex_user );

  return;
}

package Anchors;

use strict;
use warnings;
use v5.10;
use utf8;

use LWP::UserAgent;
use HTML::Strip;
use Encode;
use HTML::Restrict;
use Data::Dumper;
use POSIX;
use Clone qw( clone );
use IO::Pipe;
use Time::HiRes qw( usleep time );
use POSIX;
use JSON::XS;

use fields qw(
  ua keyword type file sites_limit pps_limit pages_processed yandex_key yandex_user
  wait_queue processing_queue processed_queue
  processed_count progress_text anchors
  wait_sentenses_queue processing_sentenses_queue processed_sentenses_queue
);

use constant PREPOSITIONS => [ 'на', 'или', 'не', 'от', 'той', 'них', 'он', 'она', 'бы', 'под', 'к', 'с' ];

sub new {
  my $proto = shift;

  my $self = fields::new( $proto );

  my $ua = LWP::UserAgent->new();
  $ua->default_header( 'Accept-Encoding' => 'utf8' );
  $ua->timeout( 4 );

  $self->{ua} = $ua;
  $self->{pages_processed} = {};
  $self->{anchors} = { 'ТВО' => {}, 'ТВРО' => {}, 'ТВР' => {}, 'НВР' => {}, 'НВРО' => {}, };

  return $self;
}

sub process {
  my __PACKAGE__ $self = shift;
  my $keyword = shift;;
  my $file = shift;
  my $sites_limit = shift;
  my $pps_limit = shift;
  my $yandex_key = shift;
  my $yandex_user = shift;

  binmode STDOUT, ':utf8';
  binmode STDERR, ':utf8';
  $| = 1;

  $self->{keyword} = decode_utf8( $keyword );
  $self->{file} = decode_utf8( $file );
  $self->{sites_limit} = $sites_limit;
  $self->{pps_limit} = $pps_limit;
  $self->{yandex_key} = $yandex_key;
  $self->{yandex_user} = $yandex_user;

  $self->_generate();

  return;
}

sub _generate {
  my __PACKAGE__ $self = shift;

  my $sites = $self->_get_sites();
  return unless $sites;

  my $anchors = $self->_get_anchors( $sites );

  my $fln = $self->{file};
  open my $fl, '>' . $fln;
  binmode $fl, ':utf8';

  foreach my $type ( keys %$anchors ) {
    say $fl $type;

    foreach my $anch ( sort keys %{ $anchors->{ $type } } ) {
      say $fl $anch;
    }

    say $fl "\n";

    say "Найдено $type:" . scalar( keys %{ $anchors->{ $type } } );
  }

  close $fl;

  return;
}

sub _get_sites {
  my __PACKAGE__ $self = shift;

  my $keyword = $self->{keyword};
  my $type = $self->{type};
  my $ua = $self->{ua};
  my $sites_limit = $self->{sites_limit};
  my $yandex_key = $self->{yandex_key};
  my $yandex_user = $self->{yandex_user};

  print "Получение данных из поиска Yandex...";

  my $str = $keyword;

  my $pages = [];
  my $progress = '';
  my $limit = ceil( $sites_limit / 10 );

  for my $cnt ( 0 .. $limit ) {
    my $rsp = $ua->get( "http://xmlsearch.yandex.com/xmlsearch?user=$yandex_user&key=$yandex_key&query=$str&lr=213&page=$cnt" );

    if ( $rsp->is_success ) {
      my $xml = $rsp->decoded_content;
      if ( $xml =~ m/error/ ) {
        $self->_clear_console( $progress );
        say 'ошибка поиска';
        my ( $err ) = $xml =~ m/\<error.*?\>(.*?)\<\/error\>/;
        say $err;
        return;
      }

      my @urls = $xml =~ m/\<url\>(.*?)\<\/url\>/g;
      push( @$pages, map( { { url => $_, level => 1, }; } @urls ) );
    }
    else {
      die $rsp->status_line;
    }

    $self->_clear_console( $progress );
    $progress = ( $cnt + 1 ) . " из $limit";
    print $progress;
  }

  $self->_clear_console( $progress );
  say 'ok';

  return $pages;
}

sub _get_sentences {
  my __PACKAGE__ $self = shift;
  my $txt = shift;

  $txt =~ s/(\<br\>|\&nbsp\;|\&quot\;|\<strong\>|\<\/strong\>|\<\/b\>|\<b\>|\<a.*?\>|\<\/a\>)/ /gio;
  $txt =~ s/(-|–|\&ndash\;)/-/gio;
  $txt =~ s/(\&laquo\;|\&raquo\;)/\"/gio;

  $txt =~ s/\s+/ /go;

  my @sentences = $txt =~ m/([АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ0-9\-\!\.\,\s…\(\)\?\:\"]+)/gio;

  @sentences = map { s/^[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ]+//io; $_; } @sentences;
  @sentences = map { s/^\s+//io; s/\s+$//io; $_; } @sentences;

  my %sentences_clear = ();

  foreach my $snt ( @sentences ) {
    next if length $snt < 40;

    my @russian_chars = $snt =~ m/([АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ])/gio;
    next if scalar( @russian_chars ) / length( $snt ) < 0.5;

    if ( length( $snt ) < 150 ) {
      $sentences_clear{ $snt } = 1;
    }
    else {
      my @snt_split = $snt =~ m/(.+?[\.\!\?])/go;
      @snt_split = map { s/^\s+//io; s/\s+$//io; $_; } @snt_split;

      foreach my $snt_split ( @snt_split ) {
        next if length $snt_split < 40;
        $sentences_clear{ $snt_split } = 1;
      }
    }
  }

  # удаляем предложения с переспамом одинаковыми словами
  foreach my $sentence ( keys %sentences_clear ) {
    my @words = $sentence =~ m/([АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ]+)/gio;
    my $skip = 0;
    my %words_count = ();

    foreach my $wrd ( @words ) {
      next if $wrd ~~ PREPOSITIONS;

      if ( $words_count{ $wrd } ) {
        $skip = 1;
        last;
      }

      $words_count{ $wrd } = 1;
    }

    delete $sentences_clear{ $sentence } if $skip;
  }

  return [ keys %sentences_clear ];
}

sub _clear_console {
  my __PACKAGE__ $self = shift;
  my $str = shift;

  my $back = "\b" x length $str;
  print $back;
  $back = " " x length $str;
  print $back;
  $back = "\b" x length $str;
  print $back;

  return;
}

sub _get_sub_pages {
  my __PACKAGE__ $self = shift;
  my $url = shift;
  my $level = shift;
  my $html = shift;
  my $count = shift;

  my $pages_processed = $self->{pages_processed};
  my $pps_limit = $self->{pps_limit};

  my @pages = ();

  my ( $domain ) = $url =~ m/^(http.?\:\/\/.+?\/)/;

  my @urls = $html =~ m/href\=\"(.*?)\"/g;

  foreach my $surl ( @urls ) {
    next if $surl =~ m/^mailto\:/;
    next if $surl =~ m/^ftp\:/;

    if ( $surl =~ m/^http/ ) {
      next unless $surl =~ m/^$domain/;

      next if $pages_processed->{ $surl };
      $pages_processed->{ $surl } = 1;

      push @pages, { url => $surl, level => $level + 1, };
      $count++;
    }
    else {
      $surl =~ s/\///;

      next if $pages_processed->{ "$domain$surl" };
      $pages_processed->{ "$domain$surl" } = 1;

      push @pages, { url => "$domain$surl", level => $level + 1, };
      $count++;
    }

    last if $count >= $pps_limit;
  }

  return @pages;
}

sub _get_anchors {
  my __PACKAGE__ $self = shift;
  my $sites = shift;

  my $sites_limit = $self->{sites_limit};

  for ( my $cnt = 1; $cnt <= scalar( @$sites ); $cnt++ ) {
    last if $cnt > $sites_limit;

    print "Обработка сайта $cnt...";

    $self->_process_site( $sites->[ $cnt - 1 ] );
    $self->_clear_console( $self->{progress_text} );

    say 'ok';
  }

  return $self->{anchors};
}

sub _get_anchors_from_sentences {
  my __PACKAGE__ $self = shift;
  my $sentences = shift;
  my $type = shift;

  my $keyword = $self->{keyword};

  my $anchors = {};

  foreach my $sent ( @$sentences ) {
    if ( $type eq 'tvo' ) {
      next unless $sent =~ m/$keyword($|[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ])/i;

      $sent =~ s/($keyword)/\#a\#$1\#\/a\#/i;

      $anchors->{ $sent } = 1;
    }
    elsif ( $type eq 'tvro' ) {
      next unless $sent =~ m/$keyword($|[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ])/i;

      if ( $sent =~ m/\s.{4,20}\s$keyword/i ) {
        $sent =~ s/\s(.{4,20}\s$keyword)/ \#a\#$1/i;
      }
      elsif ( $sent =~ m/^.{0,20}$keyword/i ) {
        $sent =~ s/^(.{0,20}$keyword)/\#a\#$1/i;
      }

      if ( $sent =~ m/$keyword[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ\w].{4,20}\s/i ) {
        $sent =~ s/($keyword[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ\w].{4,20})\s/$1\#\/a\# /i;
      }
      elsif ( $sent =~ m/$keyword.{0,20}$/i ) {
        $sent =~ s/($keyword.{0,20})$/$1\#\/a\#/i;
      }

      $anchors->{ $sent } = 1;
    }
    elsif ( $type eq 'tvr' ) {
      next unless $sent =~ m/$keyword/i;

      $sent = "#a#$sent#/a#";

      $anchors->{ $sent } = 1;
    }
    elsif ( $type eq 'nvro' ) {
      my @words = split /\s/, $keyword;
      my @words_re = ();

      foreach my $wrd ( @words ) {
        next if length $wrd <= 3;

        if ( length $wrd <= 3 ) {
          $wrd =~ s/\S{1}$/\\S\{1\,6\}/io;
        }
        if ( length $wrd <= 5 ) {
          $wrd =~ s/\S{2}$/\\S\{1\,8\}/io;
        }
        else {
          $wrd =~ s/\S{3}$/\\S\{1\,8\}/io;
        }

        push @words_re, $wrd;
      }

      my $keyword_re = join '.{1,20}', @words_re;

      next unless $sent =~ m/($keyword_re)/i;
      my $found = $1;

      next if length( $found ) < length( join ( ' ', @words_re ) ) + 7;

      @words = split /\s/, $keyword;
      my $skip = 1;
      foreach my $wrd ( @words ) {
        next if length $wrd <= 3;

        if ( $found !~ m/$wrd\s/i ) {
          $skip = 0;
          last;
        }
      }

      next if $skip;

      if ( $sent =~ m/\s.{4,20}\s\Q$found\E/i ) {
        $sent =~ s/\s(.{4,20}\s\Q$found\E)/ \#a\#$1/i;
      }
      elsif ( $sent =~ m/^.{0,20}\Q$found\E/i ) {
        $sent =~ s/^(.{0,20}\Q$found\E)/\#a\#$1/i;
      }

      if ( $sent =~ m/\Q$found\E[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ\w].{4,20}\s/i ) {
        $sent =~ s/(\Q$found\E[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ\w].{4,20})\s/$1\#\/a\# /i;
      }
      elsif ( $sent =~ m/\Q$found\E.{0,20}$/i ) {
        $sent =~ s/(\Q$found\E.{0,20})$/$1\#\/a\#/i;
      }

      $anchors->{ $sent } = 1;
    }
    elsif ( $type eq 'nvr' ) {
      my @words = split /\s/, $keyword;
      my @words_re = ();

      foreach my $wrd ( @words ) {
        next if length $wrd <= 3;

        if ( length $wrd <= 3 ) {
          $wrd =~ s/\S{1}$/\\S\{1\,6\}/io;
        }
        if ( length $wrd <= 5 ) {
          $wrd =~ s/\S{2}$/\\S\{1\,8\}/io;
        }
        else {
          $wrd =~ s/\S{3}$/\\S\{1\,8\}/io;
        }

        push @words_re, $wrd;
      }

      my $keyword_re = join '.{1,20}', @words_re;

      next unless $sent =~ m/($keyword_re)/i;
      my $found = $1;

      next if length( $found ) < length( join ( ' ', @words_re ) ) + 7;

      @words = split /\s/, $keyword;
      my $skip = 1;
      foreach my $wrd ( @words ) {
        next if length $wrd <= 3;

        if ( $found !~ m/$wrd\s/i ) {
          $skip = 0;
          last;
        }
      }

      next if $skip;

      $sent = "#a#$sent#/a#";

      $anchors->{ $sent } = 1;
    }
  }

  return [ keys %$anchors ];
}

sub _process_site {
  my __PACKAGE__ $self = shift;
  my $site = shift;

  $self->{wait_queue} = {};
  $self->{processing_queue} = {};
  $self->{processed_queue} = {};
  $self->{processed_count} = 0;
  $self->{progress_text} = '';
  $self->{wait_sentenses_queue} = [];
  $self->{processing_sentenses_queue} = {};
  $self->{processed_sentenses_queue} = {};

  my $wait_queue = $self->{wait_queue};
  my $processing_queue = $self->{processing_queue};
  my $processed_queue = $self->{processed_queue};
  my $pages_processed = $self->{pages_processed};
  my $wait_sentenses_queue = $self->{wait_sentenses_queue};
  my $processing_sentenses_queue = $self->{processing_sentenses_queue};
  my $processed_sentenses_queue = $self->{processed_sentenses_queue};

  $SIG{CHLD} = 'IGNORE';

  $wait_queue->{ $site->{url} } = { level => $site->{level}, };
  $pages_processed->{ $site->{url} } = 1;

  while (
    scalar( keys %$wait_queue ) || scalar( keys %$processing_queue ) || scalar( keys %$processed_queue )
    || scalar( @$wait_sentenses_queue ) || scalar( keys %$processing_sentenses_queue ) || scalar( keys %$processed_sentenses_queue )
  ) {
    $self->_push_to_processing_queue();
    $self->_get_from_processing_queue();

    $self->_push_to_processing_sentences_queue();
    $self->_get_from_processing_sentences_queue();

    $self->_check_childs();

    usleep( 2 );
  }

  return;
}

sub _check_childs {
  my __PACKAGE__ $self = shift;

  my $processing_queue = $self->{processing_queue};
  my $processed_queue = $self->{processed_queue};
  my $processing_sentenses_queue = $self->{processing_sentenses_queue};
  my $processed_sentenses_queue = $self->{processed_sentenses_queue};

  foreach my $pid ( keys %$processing_queue ) {
    if ( kill( 0, $pid ) == 0 ) {
      $processed_queue->{ $pid } = 1;
    }
    else {
      my $time = $processing_queue->{ $pid }->{time};

      if ( time() - $time > 5 ) {
        kill( 9, $pid );
      }
    }
  }

  foreach my $pid ( keys %$processing_sentenses_queue ) {
    if ( kill( 0, $pid ) == 0 ) {
      $processed_sentenses_queue->{ $pid } = 1;
    }
  }

  return;
}

sub _push_to_processing_queue {
  my __PACKAGE__ $self = shift;

  my $processing_queue = $self->{processing_queue};
  my $wait_queue = $self->{wait_queue};
  my $ua = $self->{ua};

  return if scalar( keys %$processing_queue ) >= 10;
  return if scalar( keys %$wait_queue ) == 0;

  foreach my $url ( keys %$wait_queue ) {
    my $level = $wait_queue->{ $url }->{level};
    delete $wait_queue->{ $url };

    my $pipe = new IO::Pipe;
    my $pid = fork;

    if ( $pid == 0 ) {
      $pipe->writer;
      $pipe->autoflush( 1 );
      $pipe->blocking( 0 );

      my $rsp = $ua->get( $url );
      exit unless $rsp->is_success;

      print $pipe encode_utf8( $rsp->decoded_content // '' );

      exit;
    }
    else {
      $pipe->reader;
      $pipe->autoflush( 1 );
      $pipe->blocking( 0 );

      $processing_queue->{ $pid } = { 'time' => time(), url => $url, pipe => $pipe, level => $level, txt => '', };
      return;
    }
  }

  return;
}

sub _get_from_processing_queue {
  my __PACKAGE__ $self = shift;

  my $processed_queue = $self->{processed_queue};
  my $processing_queue = $self->{processing_queue};
  my $processed_count = $self->{processed_count};
  my $pps_limit = $self->{pps_limit};
  my $wait_queue = $self->{wait_queue};
  my $progress_text = $self->{progress_text};
  my $wait_sentenses_queue = $self->{wait_sentenses_queue};

  return unless scalar( keys %$processed_queue ) > 0;

  foreach my $pid ( keys %$processed_queue ) {
    $processed_count++;
    my $all_count = scalar( keys %$wait_queue ) + scalar( keys %$processing_queue ) + $processed_count - 1;

    $self->_clear_console( $progress_text );
    $progress_text = "$processed_count из $all_count";
    print $progress_text;

    my $url = $processing_queue->{ $pid }->{url};
    my $time = $processing_queue->{ $pid }->{time};
    my $pipe = $processing_queue->{ $pid }->{pipe};
    my $level = $processing_queue->{ $pid }->{level};
    my $txt = $processing_queue->{ $pid }->{txt};

    delete $processing_queue->{ $pid };
    delete $processed_queue->{ $pid };

    my @lines = <$pipe>;
    if ( scalar( @lines ) ) {
      $txt .= join '', @lines;
    }

    my $html = decode_utf8( $txt );

    next unless $html;
    next if length $html > 300000;

    if ( $level <= 2 && $all_count < $pps_limit ) {
      my @subpages = $self->_get_sub_pages( $url, $level, $html, $all_count );
      map { $wait_queue->{ $_->{url} } = { level => $_->{level}, }; } @subpages;
    }

    push @$wait_sentenses_queue, $html;
  }

  $self->{processed_count} = $processed_count;
  $self->{progress_text} = $progress_text;

  return;
}

sub _push_to_processing_sentences_queue {
  my __PACKAGE__ $self = shift;

  my $processing_sentenses_queue = $self->{processing_sentenses_queue};
  my $wait_sentenses_queue = $self->{wait_sentenses_queue};
  my $anchors = { 'ТВО' => {}, 'ТВРО' => {}, 'ТВР' => {}, 'НВР' => {}, 'НВРО' => {}, };

  return if scalar( keys %$processing_sentenses_queue ) >= 3;
  return if scalar( @$wait_sentenses_queue ) == 0;

  my $txt = pop @$wait_sentenses_queue;

  my $pipe = new IO::Pipe;
  my $pid = fork;

  if ( $pid == 0 ) {
    $pipe->writer;
    $pipe->autoflush( 1 );
    $pipe->blocking( 0 );

    my $sentences = $self->_get_sentences( $txt );

    my $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'tvo' );
    map { $anchors->{ 'ТВО' }->{ $_ } = 1; } @$curr_anchors;

    $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'tvro' );
    map { $anchors->{ 'ТВРО' }->{ $_ } = 1; } @$curr_anchors;

    $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'tvr' );
    map { $anchors->{ 'ТВР' }->{ $_ } = 1; } @$curr_anchors;

    $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'nvr' );
    map { $anchors->{ 'НВР' }->{ $_ } = 1; } @$curr_anchors;

    $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'nvro' );
    map { $anchors->{ 'НВРО' }->{ $_ } = 1; } @$curr_anchors;

    print $pipe encode_utf8( encode_json( $anchors ) );

    exit;
  }
  else {
    $pipe->reader;
    $pipe->autoflush( 1 );
    $pipe->blocking( 0 );

    $processing_sentenses_queue->{ $pid } = { 'time' => time(), pipe => $pipe, };
    return;
  }

  return;
}

sub _get_from_processing_sentences_queue {
  my __PACKAGE__ $self = shift;

  my $processed_sentenses_queue = $self->{processed_sentenses_queue};
  my $processing_sentenses_queue = $self->{processing_sentenses_queue};
  my $wait_sentenses_queue = $self->{wait_sentenses_queue};
  my $anchors = $self->{anchors};

  return unless scalar( keys %$processed_sentenses_queue ) > 0;

  foreach my $pid ( keys %$processed_sentenses_queue ) {
    my $pipe = $processing_sentenses_queue->{ $pid }->{pipe};

    delete $processing_sentenses_queue->{ $pid };
    delete $processed_sentenses_queue->{ $pid };

    my $txt = '';
    my @lines = <$pipe>;
    if ( scalar( @lines ) ) {
      $txt .= join '', @lines;
    }

    my $anch = decode_json( decode_utf8( $txt ) );

    map { $anchors->{ 'ТВО' }->{ $_ } = 1; } keys %{ $anch->{ 'ТВО' } };
    map { $anchors->{ 'ТВРО' }->{ $_ } = 1; } keys %{ $anch->{ 'ТВРО' } };
    map { $anchors->{ 'ТВР' }->{ $_ } = 1; } keys %{ $anch->{ 'ТВР' } };
    map { $anchors->{ 'НВР' }->{ $_ } = 1; } keys %{ $anch->{ 'НВР' } };
    map { $anchors->{ 'НВРО' }->{ $_ } = 1; } keys %{ $anch->{ 'НВРО' } };
  }

  return;
}

1;

__END__

=head1 NAME

anchors.pl - генератор текстов анкоров

=head1 SYNOPSIS

anchors.pl [параметры]

 Обязательные параметры:
   -k, --keyword <ключевая фраза>        ключевая фраза для генерации
   -f, --file <имя файла>                файл для сохранения текстов
   -y, --yandex_key <ключ>               ключ в Yandex.XML
   -u, --yandex_user <пользователь>      пользователь в Yandex.XML

 Не обязательные параметры:
   --sites <количество>                  количество сайтов для обработки, по умолчанию - 50
   --pages <количество>                  лимит количества страниц для одного сайта, по умолчанию - 200

 Дополнительные параметры:
   -v, --version                         показать версию

=head1 DESCRIPTION

Генератор текстов анкоров

=head1 AUTHOR

Kaktus, E<lt>kak-tus@mail.ruE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013-2014 Kaktus

=cut
