package main

type VikingFrame struct {
	len                  int      //uint16
	id                   int      //byte
	destadr, srcadr, crc int      //uint16
	options              []Option //[]byte
	msgid                int      //byte
}

type Option struct {
	code, len byte
	body      []byte
}

func NewVikingFrame(id, dadr, sadr int) *VikingFrame {
	return &VikingFrame{
		id: id, destadr: dadr, srcadr: sadr,
	}
}

func (vf *VikingFrame) AddOption(code byte, body []byte) {
	op := Option{code: code, len: byte(len(body)), body: body}
	vf.options = append(vf.options, op)
}

func (vf *VikingFrame) GetBytes() []byte {
	bb := []byte{0, 0, 0, 0, 0, 0} //header
	for _, op := range vf.options {
		bb = append(bb, op.code)
		bb = append(bb, op.len)
		bb = append(bb, op.body...)
	}
	return bb
}

/*
2024-08-19 09:55:48.1511|TRACE|TcpSrv|VikingSrv [5555:0/1]  => [132] 00-80 80-00-00-00-00-
	20-запрос на регистрацию (51-PointID 02 01-96) (56-Логин 08-70-75-31-6B-70-31-35-30) (57-00 пароль)
	info 50-67 C0-CA-2D-35-30-30-20-52-54-4F-53-20-53-4E-3A-20-30-20-48-57-3A-20-31-2E-37-2E-30-20-46-57-3A-20-32-2E-35-2E-37-20-4D-4F-44-45-4D-3A-20-31-32-2E-30-30-2E-36-31-36-20-49-4D-45-49-3A-20-33-35-35-38-35-35-30-35-36-31-33-38-38-35-30-20-49-43-43-49-44-31-3A-20-38-39-37-30-31-30-32-38-33-34-38-31-30-39-36-34-39-32) FF-F7-1D
2024-08-19 09:55:48.1511|DEBUG|TcpSrv|VikingSrv [5555:0/1]  >ReqRegister_PointID:0196 ??-500 RTOS SN: 0 HW: 1.7.0 FW: 2.5.7 MODEM: 12.00.616 IMEI: 355855056138850 ICCID1: 897010283481096492
2024-08-19 09:55:48.1511|TRACE|TcpSrv|VikingSrv [5555:0/1]  <= [18] 00-0E-80-00-00-00-00-21-52-02-01-96-55-01-04-FF-52-F0

*/

