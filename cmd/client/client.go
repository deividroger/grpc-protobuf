package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"gitub.com/deividroger/grpc-protobuf/pb"
	"google.golang.org/grpc"
)

func main() {
	connection, err := grpc.Dial("localhost:50051", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Could not connect grpc Server %v", err)
	}
	defer connection.Close()

	client := pb.NewUserServiceClient(connection)

	//AddUser(client)
	//AddUserVerbose(client)
	//AddUsers(client)
	AddUserStreamBoth(client)

}

func AddUser(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "Deivid",
		Email: "deivid@deividroger.net",
	}

	res, err := client.AddUser(context.Background(), req)

	if err != nil {
		log.Fatalf("Could not make grpc request %v", err)
	}

	fmt.Println(res)
}

func AddUserVerbose(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "Deivid",
		Email: "deivid@deividroger.net",
	}

	responseStream, err := client.AddUserVerbose(context.Background(), req)

	if err != nil {
		log.Fatalf("Could not make grpc request %v", err)
	}

	for {
		stream, error := responseStream.Recv()

		if error == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Could not receive the msg: %v", err)
		}

		fmt.Println("Status:", stream.Status, " - ", stream.GetUser())

	}
}

func AddUsers(client pb.UserServiceClient) {

	stream, err := client.AddUsers(context.Background())

	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	for _, req := range GetUserData() {
		stream.Send(req)
		time.Sleep(time.Second * 3)
	}

	res, err := stream.CloseAndRecv()

	if err != nil {
		log.Fatalf("Error receiving response %v", err)
	}
	fmt.Println(res)
}

func AddUserStreamBoth(client pb.UserServiceClient) {

	stream, err := client.AddUserStreamBoth(context.Background())

	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	go func() {

		for _, req := range GetUserData() {
			fmt.Println("Sending user: ", req.Name)
			stream.Send(req)
			time.Sleep(time.Second * 2)
		}
		stream.CloseSend()
	}()

	wait := make(chan int)

	go func() {
		for {
			res, err := stream.Recv()

			if err == io.EOF {
				break
			}

			if err != nil {
				log.Fatalf("Error receiving data: %v", err)
				break
			}

			fmt.Printf("Recebendo user %v com status %v\n", res.GetUser().GetName(), res.GetStatus())

		}
		close(wait)
	}()

	<-wait
}

func GetUserData() []*pb.User {
	dataUser := []*pb.User{
		&pb.User{
			Id:    "d1",
			Name:  "Roger 1",
			Email: "Roger@roger.com",
		},
		&pb.User{
			Id:    "d2",
			Name:  "Roger 2",
			Email: "deivid@santos.com.br",
		},
		&pb.User{
			Id:    "d3",
			Name:  "Roger 3",
			Email: "Oliveira@oliveira.com.br",
		},
		&pb.User{
			Id:    "d4",
			Name:  "Roger 4",
			Email: "santos@santos.com.br",
		},
	}
	return dataUser
}
